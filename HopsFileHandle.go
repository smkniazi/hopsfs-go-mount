// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

// Represends a handle to an open file
type FileHandle struct {
	File              *File
	Mutex             sync.Mutex     // all operations on the handle are serialized to simplify invariants
	fileFlags         fuse.OpenFlags // flags used to creat the file
	tatalBytesRead    int64
	totalBytesWritten int64
}

// Verify that *FileHandle implements necesary FUSE interfaces
var _ fs.Node = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)
var _ fs.NodeFsyncer = (*FileHandle)(nil)
var _ fs.HandleFlusher = (*FileHandle)(nil)

func (fh *FileHandle) createStagingFile(operation string, existsInDFS bool) error {
	if fh.File.handle != nil {
		return nil // there is already an active handle.
	}

	//create staging file
	absPath := fh.File.AbsolutePath()
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	if !existsInDFS { // it  is a new file so create it in the DFS
		w, err := hdfsAccessor.CreateFile(absPath, fh.File.Attrs.Mode, false)
		if err != nil {
			logerror("Failed to create file in DFS", Fields{Operation: operation, Path: absPath, Error: err})
			return err
		}
		loginfo("Created an empty file in DFS", Fields{Operation: operation, Path: absPath})
		w.Close()
	} else {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(absPath)
		if err != nil {
			logerror("Failed to stat file in DFS", Fields{Operation: operation, Path: absPath, Error: err})
			return &os.PathError{Op: operation, Path: absPath, Err: os.ErrNotExist}
		}
	}

	stagingFile, err := ioutil.TempFile(stagingDir, "stage")
	if err != nil {
		logerror("Failed to create staging file", Fields{Operation: operation, Path: absPath, Error: err})
		return err
	}
	os.Remove(stagingFile.Name())
	loginfo("Created staging file", Fields{Operation: operation, Path: absPath, TmpFile: stagingFile.Name()})

	if existsInDFS {
		if err := fh.downloadToStaging(stagingFile, operation); err != nil {
			return err
		}
	}
	fh.File.handle = stagingFile
	return nil
}

func (fh *FileHandle) downloadToStaging(stagingFile *os.File, operation string) error {
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	absPath := fh.File.AbsolutePath()

	reader, err := hdfsAccessor.OpenRead(absPath)
	if err != nil {
		logerror("Failed to open file in DFS", Fields{Operation: operation, Path: absPath, Error: err})
		// TODO remove the staging file if there are no more active handles
		return &os.PathError{Op: operation, Path: absPath, Err: err}
	}

	nc, err := io.Copy(stagingFile, reader)
	if err != nil {
		logerror("Failed to copy content to staging file", Fields{Operation: operation, Path: absPath, Error: err})
		return &os.PathError{Op: operation, Path: absPath, Err: err}
	}
	reader.Close()
	loginfo(fmt.Sprintf("Downloaded a copy to stating dir. %d bytes copied", nc), Fields{Operation: operation, Path: fh.File.AbsolutePath()})
	return nil
}

// Creates new file handle
func NewFileHandle(file *File, existsInDFS bool, flags fuse.OpenFlags) (*FileHandle, error) {

	operation := Create
	if existsInDFS {
		operation = Open
	}

	fh := &FileHandle{File: file, fileFlags: flags}
	checkDiskSpace(fh.File.FileSystem.HdfsAccessor)

	if err := fh.createStagingFile(operation, existsInDFS); err != nil {
		return nil, err
	}

	loginfo("Opened file", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Flags: fh.fileFlags})
	return fh, nil
}

func (fh *FileHandle) isWriteable() bool {
	if fh.fileFlags.IsWriteOnly() || fh.fileFlags.IsReadWrite() {
		return true
	} else {
		return false
	}
}

func (fh *FileHandle) Truncate(size int64) error {
	err := fh.File.handle.Truncate(size)
	if err != nil {
		logerror("Failed to truncate file", Fields{Operation: Truncate, Path: fh.File.AbsolutePath(), Bytes: size, Error: err})
	}
	loginfo("Truncated file", Fields{Operation: Truncate, Path: fh.File.AbsolutePath(), Bytes: size})
	return nil
}

func checkDiskSpace(hdfsAccessor HdfsAccessor) error {
	//TODO FIXME
	//	fsInfo, err := hdfsAccessor.StatFs()
	//	if err != nil {
	//		// Donot abort, continue writing
	//		Error.Println("Failed to get HDFS usage, ERROR:", err)
	//	} else if uint64(req.Offset) >= fsInfo.remaining {
	//		Error.Println("[", fhw.Handle.File.AbsolutePath(), "] writes larger size (", req.Offset, ")than HDFS available size (", fsInfo.remaining, ")")
	//		return errors.New("Too large file")
	//	}
	return nil
}

// Returns attributes of the file associated with this handle
func (fh *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	return fh.File.Attr(ctx, a)
}

// Responds to FUSE Read request
func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()

	buf := resp.Data[0:req.Size]
	nr, err := fh.File.handle.ReadAt(buf, req.Offset)
	resp.Data = buf[0:nr]
	fh.tatalBytesRead += int64(nr)

	if err != nil {
		if err == io.EOF {
			// EOF isn't a error, reporting successful read to FUSE
			logdebug("Finished reading from staging file. EOF", Fields{Operation: Read, Path: fh.File.AbsolutePath(), Bytes: nr})
			return nil
		} else {
			logerror("Failed to read from staging file", Fields{Operation: Read, Path: fh.File.AbsolutePath(), Error: err, Bytes: nr})
			return err
		}
	}
	logdebug("Read from staging file", Fields{Operation: Read, Path: fh.File.AbsolutePath(), Bytes: nr, ReqOffset: req.Offset})
	return err
}

// Responds to FUSE Write request
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()

	nw, err := fh.File.handle.WriteAt(req.Data, req.Offset)
	resp.Size = nw
	fh.totalBytesWritten += int64(nw)
	if err != nil {
		logerror("Failed to write to staging file", Fields{Operation: Write, Path: fh.File.AbsolutePath(), Error: err})
		return err
	} else {
		logdebug("Write data to staging file", Fields{Operation: Write, Path: fh.File.AbsolutePath(), Bytes: nw})
		return nil
	}
}

func (fh *FileHandle) copyToDFS(operation string) error {
	if fh.totalBytesWritten == 0 { // Nothing to do
		return nil
	}
	defer fh.File.InvalidateMetadataCache()

	logdebug("Uploading to DFS", Fields{Operation: Write, Path: fh.File.AbsolutePath(), Bytes: TotalBytesWritten})

	op := fh.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := fh.FlushAttempt(operation)
		if err != io.EOF || IsSuccessOrBenignError(err) || !op.ShouldRetry("Flush()", err) {
			return err
		}
		// Reconnect and try again
		fh.File.FileSystem.HdfsAccessor.Close()
		logwarn("Failed to copy file to DFS", Fields{Operation: operation, Path: fh.File.AbsolutePath()})
	}
}

func (fh *FileHandle) FlushAttempt(operation string) error {
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	w, err := hdfsAccessor.CreateFile(fh.File.AbsolutePath(), fh.File.Attrs.Mode, true)
	if err != nil {
		logerror("Error creating file in DFS", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err})
		return err
	}

	//open the file for reading and upload to DFS
	offset, err := fh.File.handle.Seek(0, 0)
	if err != nil || offset != 0 {
		logerror("Unable to seek to the begenning of the temp file", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Offset: offset, Error: err})
		return err
	}

	b := make([]byte, 65536)
	written := 0
	for {
		nr, err := fh.File.handle.Read(b)
		if err != nil {
			if err != io.EOF {
				logerror("Failed to read from staging file", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err})
			}
			break
		}
		b = b[:nr]

		nw, err := w.Write(b)
		if err != nil {
			logerror("Failed to write to DFS", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err})
			w.Close()
			return err
		}
		logtrace("Written to DFS", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Bytes: nw})
		written += nw
	}

	err = w.Close()
	if err != nil {
		logerror("Failed to close file in DFS", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Error: err})
		return err
	}
	loginfo("Uploaded to DFS", Fields{Operation: operation, Path: fh.File.AbsolutePath(), Bytes: written})
	return nil
}

// Responds to the FUSE Flush request
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	if fh.isWriteable() {
		loginfo("Flush file", Fields{Operation: Flush, Path: fh.File.AbsolutePath()})
		return fh.copyToDFS(Flush)
	} else {
		return nil
	}
}

// Responds to the FUSE Fsync request
func (fh *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	if fh.isWriteable() {
		loginfo("Fsync file", Fields{Operation: Fsync, Path: fh.File.AbsolutePath()})
		return fh.copyToDFS(Fsync)
	} else {
		return nil
	}
}

// Closes the handle
func (fh *FileHandle) Release(_ context.Context, _ *fuse.ReleaseRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()

	//close the file handle if it is the last handle
	fh.File.InvalidateMetadataCache()
	fh.File.RemoveHandle(fh)
	activeHandles := fh.File.countActiveHandles()
	if activeHandles == 0 {
		err := fh.File.handle.Close()
		if err != nil {
			logerror("Failed to close staging file", Fields{Operation: Close, Path: fh.File.AbsolutePath(), Error: err})
		}
		fh.File.handle = nil
		loginfo("Staging file is closed", Fields{Operation: Close, Path: fh.File.AbsolutePath(), Flags: fh.fileFlags, TotalBytesRead: fh.tatalBytesRead, TotalBytesWritten: fh.totalBytesWritten})
	} else {
		logdebug("Staging file is not closed becuase it has ", Fields{Operation: Close, Path: fh.File.AbsolutePath(), Flags: fh.fileFlags, TotalBytesRead: fh.tatalBytesRead, TotalBytesWritten: fh.totalBytesWritten})
	}

	loginfo("Close file", Fields{Operation: Close, Path: fh.File.AbsolutePath(), Flags: fh.fileFlags, TotalBytesRead: fh.tatalBytesRead, TotalBytesWritten: fh.totalBytesWritten})
	return nil
}
