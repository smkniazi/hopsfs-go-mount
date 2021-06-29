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

	logger "github.com/sirupsen/logrus"
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
	logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path}).Infof("Creating file write handle. Newfile %t", newFile)

	hdfsAccessor := fhw.Handle.File.FileSystem.HdfsAccessor
	if newFile {
		w, err := hdfsAccessor.CreateFile(path, fhw.Handle.File.Attrs.Mode, true)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, Error: err}).Warn("Failed to create file in DFS")
			return nil, err
		}
		w.Close()
	}

	if err := os.MkdirAll(stagingDir, 0700); err != nil {
		logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, Error: err}).Warn("Failed to create staging dir")
		return nil, err
	}
	var err error
	fhw.stagingFile, err = ioutil.TempFile(stagingDir, "stage")
	if err != nil {
		return nil, err
	}
	// os.Remove(this.stagingFile.Name()) //TODO: handle error

	logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, TmpFile: fhw.stagingFile.Name()}).Info("Created Staging file")
	if !newFile {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(path)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, Error: err}).Warn("Failed to stat file in DFS")
			return fhw, nil
		}

		reader, err := hdfsAccessor.OpenRead(path)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, Error: err}).Warn("Failed to open file in DFS")
			fhw.stagingFile.Close()
			fhw.stagingFile = nil
			return nil, err
		}
		nc, err := io.Copy(fhw.stagingFile, reader)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, TmpFile: fhw.stagingFile.Name(), Error: err}).Warn("Failed to copy file from DFS")
			fhw.stagingFile.Close()
			fhw.stagingFile = nil
			return nil, err
		}
		reader.Close()
		logger.WithFields(logger.Fields{Operation: WriteHandle, Path: path, TmpFile: fhw.stagingFile.Name(), Bytes: nc}).Info("Copied data to staging file")
	}

	return fhw, nil
}

// Responds on FUSE Write request
func (fhw *FileHandleWriter) Write(handle *FileHandle, ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fsInfo, err := fhw.Handle.File.FileSystem.HdfsAccessor.StatFs()
	if err != nil {
		// Donot abort, continue writing
		logger.WithFields(logger.Fields{Operation: Write, Path: handle.File.AbsolutePath(), Error: err}).Warn("Failed to get DFS usage")
	} else if uint64(req.Offset) >= fsInfo.remaining {
		logger.WithFields(logger.Fields{Operation: Write, Path: handle.File.AbsolutePath()}).Error("Too large file")
		return errors.New("Too large file")
	}

	nw, err := fhw.stagingFile.WriteAt(req.Data, req.Offset)
	resp.Size = nw
	if err != nil {
		return err
	}
	fhw.BytesWritten += uint64(nw)

	logger.WithFields(logger.Fields{Operation: Write, Path: handle.File.AbsolutePath(), Bytes: nw}).Error("Written data")
	return nil
}

// Responds on FUSE Flush/Fsync request
func (fhw *FileHandleWriter) Flush() error {
	if fhw.BytesWritten == 0 {
		// Nothing to do
		return nil
	}
	fhw.BytesWritten = 0
	defer fhw.Handle.File.InvalidateMetadataCache()

	op := fhw.Handle.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := fhw.FlushAttempt()
		logger.WithFields(logger.Fields{Operation: Flush, Path: fhw.Handle.File.AbsolutePath(), Bytes: fhw.BytesWritten}).Info("Flushed data")
		if err != io.EOF || IsSuccessOrBenignError(err) || !op.ShouldRetry("Flush()", err) {
			return err
		}
		// Restart a new connection, https://github.com/colinmarc/hdfs/issues/86
		fhw.Handle.File.FileSystem.HdfsAccessor.Close()
		logger.WithFields(logger.Fields{Operation: Flush, Path: fhw.Handle.File.AbsolutePath(), Error: err}).Warn("Flushed failed")
		// Wait for 30 seconds before another retry to get another set of datanodes.
		// https://community.hortonworks.com/questions/2474/how-to-identify-stale-datanode.html
		time.Sleep(30 * time.Second)
	}
	return nil
}

// Single attempt to flush a file
func (fhw *FileHandleWriter) FlushAttempt() error {
	hdfsAccessor := fhw.Handle.File.FileSystem.HdfsAccessor
	w, err := hdfsAccessor.CreateFile(fhw.Handle.File.AbsolutePath(), fhw.Handle.File.Attrs.Mode, true)
	if err != nil {
		logger.WithFields(logger.Fields{Operation: Flush, Path: fhw.Handle.File.AbsolutePath(), Error: err}).Warn("Error creating file in DFS")
		return err
	}

	fhw.stagingFile.Seek(0, 0)
	b := make([]byte, 65536)
	for {
		nr, err := fhw.stagingFile.Read(b)
		if err != nil {
			break
		}
		b = b[:nr]

		_, err = w.Write(b)
		if err != nil {
			logger.WithFields(logger.Fields{Operation: Flush, Path: fhw.Handle.File.AbsolutePath(), Error: err}).Warn("Error writing file in DFS")
			w.Close()
			return err
		}

	}
	err = w.Close()
	if err != nil {
		logger.WithFields(logger.Fields{Operation: Flush, Path: fhw.Handle.File.AbsolutePath(), Error: err}).Warn("Error closing file in DFS")
		return err
	}

	return nil
}

// Closes the writer
func (fhw *FileHandleWriter) Close() error {
	logger.WithFields(logger.Fields{Operation: Flush, Path: fhw.Handle.File.AbsolutePath()}).Warn("Closing staging file")
	return fhw.stagingFile.Close()
}
