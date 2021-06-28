// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"io"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

// Represends a handle to an open file
type FileHandle struct {
	File      *File
	Mutex     sync.Mutex // all operations on the handle are serialized to simplify invariants
	handle    *os.File
	fileFlags fuse.OpenFlags // flags used to creat the file
}

// Verify that *FileHandle implements necesary FUSE interfaces
var _ fs.Node = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)
var _ fs.NodeFsyncer = (*FileHandle)(nil)
var _ fs.HandleFlusher = (*FileHandle)(nil)

func (fh *FileHandle) createStagingFile(existsInDFS bool) error {
	if fh.File.tmpFile != "" {
		if _, err := os.Stat(fh.File.tmpFile); err == nil {
			return nil
		}
	}

	//create staging file
	absPath := fh.File.AbsolutePath()
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	if !existsInDFS { // it  is a new file so create it in the DFS
		w, err := hdfsAccessor.CreateFile(absPath, fh.File.Attrs.Mode, false)
		if err != nil {
			Error.Printf("Failed to create the file in HopsFS. Path: %s Error: %v\n", absPath, err)
			return err
		}
		w.Close()
	} else {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(absPath)
		if err != nil {
			Warning.Printf("Creating file failed. Stat failed for %s. Error: %v", absPath, err)
			return &os.PathError{Op: "open", Path: absPath, Err: os.ErrNotExist}
		}
	}

	stagingFile, err := ioutil.TempFile(stagingDir, "stage")
	if err != nil {
		return err
	}
	defer stagingFile.Close()
	Info.Printf("Creating staging file for %s, staging file: %s ", absPath, stagingFile.Name())

	fh.File.tmpFile = stagingFile.Name()
	return nil
}

func (fh *FileHandle) downloadToStaging() error {
	Info.Printf("%s already exists in the DFS. Downloading a copy to stating dir ", fh.File.AbsolutePath())
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	absPath := fh.File.AbsolutePath()

	reader, err := hdfsAccessor.OpenRead(absPath)
	if err != nil {
		Warning.Printf("Failed to open HopsFS file: %s Error: %v\n", absPath, err)
		// TODO remove the staging file if there are no more active handles
		return &os.PathError{Op: "open", Path: absPath, Err: err}
	}

	tmpFileHandle, err := os.OpenFile(fh.File.tmpFile, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		Warning.Printf("Failed to open temp file: %s\n", fh.File.tmpFile)
		return &os.PathError{Op: "open", Path: absPath, Err: err}
	}

	nc, err := io.Copy(tmpFileHandle, reader)
	if err != nil {
		Warning.Printf("Failed to write to temp file: %s. Error: %v\n", fh.File.tmpFile, err)
		tmpFileHandle.Close()
		// TODO remote the statging file.
		return &os.PathError{Op: "open", Path: absPath, Err: err}
	}
	reader.Close()
	tmpFileHandle.Close()
	Info.Printf("Buffering the contents of the file %s to the staging file %s. %d bytes copied\n", absPath, fh.File.tmpFile, nc)
	return nil
}

// Creates new file handle
func NewFileHandle(file *File, existsInDFS bool, flags fuse.OpenFlags) (*FileHandle, error) {

	fh := &FileHandle{File: file, fileFlags: flags}
	checkDiskSpace(fh.File.FileSystem.HdfsAccessor)

	if err := fh.createStagingFile(existsInDFS); err != nil {
		return nil, err
	}

	//	isTrunc := false
	//	isAppend := false
	//	if (flags | fuse.OpenTruncate) == fuse.OpenTruncate {
	//		isTrunc = true
	//	}
	//	if (flags | fuse.OpenAppend) == fuse.OpenAppend {
	//		isAppend = true
	//	}
	truncateContent := flags.IsWriteOnly() && (flags&fuse.OpenAppend != fuse.OpenAppend)

	if existsInDFS && !truncateContent {
		// TODO handle the case of truncate.
		if err := fh.downloadToStaging(); err != nil {
			return nil, err
		}
	}

	if truncateContent {
		Info.Printf("Truncated file %s\n", fh.File.AbsolutePath())
		os.Truncate(fh.File.tmpFile, 0)
	}

	// remove the O_EXCL flag as the staging file is now created.
	//int((flags&^fuse.OpenExclusive)|fuse.OpenReadWrite)
	Info.Printf("Old Flags %v, New flags %v ", flags, (flags &^ fuse.OpenExclusive))
	fileHandle, err := os.OpenFile(fh.File.tmpFile, int(fuse.OpenReadWrite), 0600)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: fh.File.tmpFile, Err: err}
	}

	fh.handle = fileHandle
	return fh, nil
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
	_, err := fh.handle.Seek(req.Offset, 0)
	if err != nil {
		return err
	}

	nr, err := fh.handle.Read(buf)
	resp.Data = resp.Data[0:nr]

	if err != nil {
		if err == io.EOF {
			// EOF isn't a error, reporting successful read to FUSE
			Warning.Printf("Read request for %s. Read: %d bytes.\n", fh.File.AbsolutePath(), nr)
			return nil
		} else {
			Info.Printf("Read request for %s. Error: %v.\n", fh.File.AbsolutePath(), err)
			return err
		}
	}
	Info.Printf("Read request for %s. Read bytes: %d.\n", fh.File.AbsolutePath(), nr)
	return err
}

// Responds to FUSE Write request
func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()

	var nw int
	var err error
	// if fh.fileFlags&fuse.OpenAppend == fuse.OpenAppend {
	// Info.Println("using write API")
	// nw, err = fh.handle.Write(req.Data)
	// } else {
	// Info.Println("using writeAt API")
	nw, err = fh.handle.WriteAt(req.Data, req.Offset)
	// }
	resp.Size = nw
	if err != nil {
		Warning.Printf("Write request for %s. Error: %v.", fh.File.AbsolutePath(), err)
		return err
	} else {
		Info.Printf("Write request for %s. Written: %d bytes.", fh.File.AbsolutePath(), nw)
		return nil
	}
}

func (fh *FileHandle) copyToDFS() error {
	info, _ := os.Stat(fh.File.tmpFile)
	var size int64 = 0
	if info != nil {
		size = info.Size()
	}
	Info.Printf("Uploading data to %s. File Size: %d", fh.File.AbsolutePath(), size)

	if size == 0 { // Nothing to do
		return nil
	}
	defer fh.File.InvalidateMetadataCache()

	op := fh.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := fh.FlushAttempt()
		Info.Println("[", fh.File.AbsolutePath(), "] flushed (", size, " bytes)")
		if err != io.EOF || IsSuccessOrBenignError(err) || !op.ShouldRetry("Flush()", err) {
			return err
		}
		// Restart a new connection, https://github.com/colinmarc/hdfs/issues/86
		fh.File.FileSystem.HdfsAccessor.Close()
		Error.Printf("%s failed flushing. Error %v. Retrying...", fh.File.AbsolutePath(), err)
		// Wait for 30 seconds before another retry to get another set of datanodes.
		// https://community.hortonworks.com/questions/2474/how-to-identify-stale-datanode.html
		time.Sleep(30 * time.Second)
	}
	return nil
}

func (fh *FileHandle) FlushAttempt() error {
	Info.Printf("%s flush attempt ", fh.File.AbsolutePath())
	hdfsAccessor := fh.File.FileSystem.HdfsAccessor
	w, err := hdfsAccessor.CreateFile(fh.File.AbsolutePath(), fh.File.Attrs.Mode, true)
	if err != nil {
		Error.Println("ERROR creating", fh.File.AbsolutePath(), ":", err)
		return err
	}

	//open the file for reading and upload to DFS
	in, err := os.Open(fh.File.tmpFile)
	defer in.Close()

	if err != nil {
		return &os.PathError{Op: "Write", Path: fh.File.AbsolutePath(), Err: err}
	}

	b := make([]byte, 65536)
	for {
		nr, err := in.Read(b)
		if err != nil {
			if err != io.EOF {
				Warning.Printf("%s Error in reading. Error %v", fh.File.AbsolutePath(), err)
			}
			break
		}
		b = b[:nr]

		nw, err := w.Write(b)
		if err != nil {
			Error.Println("Writing", fh.File.AbsolutePath(), ":", err)
			w.Close()
			return err
		}
		Info.Printf("%s  byte to Write %d, bytes writtedn %d", fh.File.AbsolutePath(), len(b), nw)

	}

	err = w.Close()
	if err != nil {
		Error.Println("Closing", fh.File.AbsolutePath(), ":", err)
		return err
	}
	return nil
}

// Responds to the FUSE Flush request
func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	Info.Printf("Flush file %s. ", fh.File.AbsolutePath())
	return fh.copyToDFS()
}

// Responds to the FUSE Fsync request
func (fh *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	Info.Printf("Sync file %s. ", fh.File.AbsolutePath())
	return fh.copyToDFS()
}

// Closes the handle
func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fh.Mutex.Lock()
	defer fh.Mutex.Unlock()
	err := fh.handle.Close()

	if err != nil {
		Warning.Printf("%s closed %v", fh.File.AbsolutePath(), err)
	} else {
		Info.Printf("%s closed", fh.File.AbsolutePath())
	}
	fh.handle = nil
	fh.File.InvalidateMetadataCache()
	fh.File.RemoveHandle(fh)
	return err
}
