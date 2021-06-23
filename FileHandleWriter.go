// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"io"
	"io/ioutil"
	"os"
	"time"

	"bazil.org/fuse"
	"golang.org/x/net/context"
)

// Encapsulates state and routines for writing data from the file handle
type FileHandleWriter struct {
	Handle       *FileHandle
	stagingFile  *os.File
	BytesWritten uint64
}

// Opens the file for writing
func NewFileHandleWriter(handle *FileHandle, newFile bool) (*FileHandleWriter, error) {
	fhw := &FileHandleWriter{Handle: handle}
	path := fhw.Handle.File.AbsolutePath()
	Info.Printf("Create file %s, newFile: %t ", path, newFile)

	hdfsAccessor := fhw.Handle.File.FileSystem.HdfsAccessor
	Info.Println("Attr is ", fhw.Handle.File.Attrs)
	if newFile {
		hdfsAccessor.Remove(path)
		w, err := hdfsAccessor.CreateFile(path, fhw.Handle.File.Attrs.Mode)
		if err != nil {
			Error.Println("Creating", path, ":", path, err)
			return nil, err
		}
		w.Close()
	}

	if ok := os.MkdirAll(stagingDir, 0700); ok != nil {
		Error.Println("Failed to create stageDir", stagingDir, ", Error:", ok)
		return nil, ok
	}
	var err error
	fhw.stagingFile, err = ioutil.TempFile(stagingDir, "stage")
	if err != nil {
		return nil, err
	}
	// os.Remove(this.stagingFile.Name()) //TODO: handle error

	Info.Printf("Stagaing file for %s is %s", fhw.Handle.File.Attrs.Name, fhw.stagingFile.Name())

	if !newFile {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(path)
		if err != nil {
			Warning.Println("[", path, "] Can't stat file:", err)
			return fhw, nil
		}

		Info.Printf("Buffering contents of the file %s to the staging area %s", fhw.Handle.File.Attrs.Name, fhw.stagingFile.Name())
		reader, err := hdfsAccessor.OpenRead(path)
		if err != nil {
			Warning.Println("HDFS/open failure:", err)
			fhw.stagingFile.Close()
			fhw.stagingFile = nil
			return nil, err
		}
		nc, err := io.Copy(fhw.stagingFile, reader)
		if err != nil {
			Warning.Println("Copy failure:", err)
			fhw.stagingFile.Close()
			fhw.stagingFile = nil
			return nil, err
		}
		reader.Close()
		Info.Println("Copied", nc, "bytes")
	}

	return fhw, nil
}

// Responds on FUSE Write request
func (fhw *FileHandleWriter) Write(handle *FileHandle, ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fsInfo, err := fhw.Handle.File.FileSystem.HdfsAccessor.StatFs()
	if err != nil {
		// Donot abort, continue writing
		Error.Println("Failed to get HDFS usage, ERROR:", err)
	} else if uint64(req.Offset) >= fsInfo.remaining {
		Error.Println("[", fhw.Handle.File.AbsolutePath(), "] writes larger size (", req.Offset, ")than HDFS available size (", fsInfo.remaining, ")")
		return errors.New("Too large file")
	}

	nw, err := fhw.stagingFile.WriteAt(req.Data, req.Offset)
	resp.Size = nw
	if err != nil {
		return err
	}
	fhw.BytesWritten += uint64(nw)

	Info.Printf("%s write %d bytes", handle.File.Attrs.Name, nw)
	return nil
}

// Responds on FUSE Flush/Fsync request
func (fhw *FileHandleWriter) Flush() error {
	Info.Println("[", fhw.Handle.File.AbsolutePath(), "] flushing (", fhw.BytesWritten, "new bytes written)")
	if fhw.BytesWritten == 0 {
		// Nothing to do
		return nil
	}
	fhw.BytesWritten = 0
	defer fhw.Handle.File.InvalidateMetadataCache()

	op := fhw.Handle.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := fhw.FlushAttempt()
		Info.Println("[", fhw.Handle.File.AbsolutePath(), "] flushed (", fhw.BytesWritten, "new bytes written)")
		if err != io.EOF || IsSuccessOrBenignError(err) || !op.ShouldRetry("Flush()", err) {
			return err
		}
		// Restart a new connection, https://github.com/colinmarc/hdfs/issues/86
		fhw.Handle.File.FileSystem.HdfsAccessor.Close()
		Error.Println("[", fhw.Handle.File.AbsolutePath(), "] failed flushing. Retry")
		// Wait for 30 seconds before another retry to get another set of datanodes.
		// https://community.hortonworks.com/questions/2474/how-to-identify-stale-datanode.html
		time.Sleep(30 * time.Second)
	}
	return nil
}

// Single attempt to flush a file
func (fhw *FileHandleWriter) FlushAttempt() error {
	hdfsAccessor := fhw.Handle.File.FileSystem.HdfsAccessor
	hdfsAccessor.Remove(fhw.Handle.File.AbsolutePath())
	w, err := hdfsAccessor.CreateFile(fhw.Handle.File.AbsolutePath(), fhw.Handle.File.Attrs.Mode)
	if err != nil {
		Error.Println("ERROR creating", fhw.Handle.File.AbsolutePath(), ":", err)
		return err
	}

	fhw.stagingFile.Seek(0, 0)
	b := make([]byte, 65536, 65536)
	for {
		nr, err := fhw.stagingFile.Read(b)
		if err != nil {
			break
		}
		b = b[:nr]

		_, err = w.Write(b)
		if err != nil {
			Error.Println("Writing", fhw.Handle.File.AbsolutePath(), ":", err)
			w.Close()
			return err
		}

	}
	err = w.Close()
	if err != nil {
		Error.Println("Closing", fhw.Handle.File.AbsolutePath(), ":", err)
		return err
	}

	return nil
}

// Closes the writer
func (fhw *FileHandleWriter) Close() error {
	Info.Printf("Closing staging file %s", fhw.stagingFile.Name())
	return fhw.stagingFile.Close()
}
