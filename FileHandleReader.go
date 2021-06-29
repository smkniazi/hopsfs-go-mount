// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"io"

	"bazil.org/fuse"
	logger "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Encapsulates state and routines for reading data from the file handle
// FileHandleReader implements simple two-buffer scheme which allows to efficiently
// handle unordered reads which aren't far away from each other, so backend stream can
// be read sequentially without seek
type FileHandleReader struct {
	Handle     *FileHandle    // File handle
	HdfsReader ReadSeekCloser // Backend reader
	Offset     int64          // Current offset for backend reader
	Buffer1    *FileFragment  // Most recent fragment from the backend reader
	Buffer2    *FileFragment  // Least recent fragment read from the backend
	Holes      int64          // tracks number of encountered "holes" TODO: find better name
	CacheHits  int64          // tracks number of cache hits (read requests from buffer)
	Seeks      int64          // tracks number of seeks performed on the backend stream
}

// Opens the reader (creates backend reader)
func NewFileHandleReader(handle *FileHandle) (*FileHandleReader, error) {
	this := &FileHandleReader{Handle: handle}
	var err error
	this.HdfsReader, err = handle.File.FileSystem.HdfsAccessor.OpenRead(handle.File.AbsolutePath())
	if err != nil {
		logger.WithFields(logger.Fields{Operation: ReadHandle, Path: handle.File.AbsolutePath(), Error: err}).Error("Failed to open file in DFS")
		return nil, err
	}
	this.Buffer1 = &FileFragment{}
	this.Buffer2 = &FileFragment{}
	logger.WithFields(logger.Fields{Operation: ReadHandle, Path: handle.File.AbsolutePath()}).Info("Reader handle created")
	return this, nil
}

// Responds on FUSE Read request. Note: If FUSE requested to read N bytes it expects exactly N, unless EOF
func (fhr *FileHandleReader) Read(handle *FileHandle, ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	totalRead := 0
	buf := resp.Data[0:req.Size]
	fileOffset := req.Offset
	var nr int
	var err error
	for len(buf) > 0 {
		nr, err = fhr.ReadPartial(handle, fileOffset, buf)
		if err != nil {
			break
		}
		logger.WithFields(logger.Fields{Operation: Read, Path: handle.File.AbsolutePath(), Bytes: totalRead}).Trace("Read chunk")
		totalRead += nr
		fileOffset += int64(nr)
		buf = buf[nr:]
	}
	resp.Data = resp.Data[0:totalRead]
	if err != nil && err != io.EOF {
		logger.WithFields(logger.Fields{Operation: Read, Path: handle.File.AbsolutePath(), Error: err}).Warn("Read failed")
		return err
	}
	logger.WithFields(logger.Fields{Operation: Read, Path: handle.File.AbsolutePath(), Bytes: totalRead}).Info("Read")
	return nil
}

var BLOCKSIZE int = 65536

// Reads chunk of data (satisfies part of FUSE read request)
func (fhr *FileHandleReader) ReadPartial(handle *FileHandle, fileOffset int64, buf []byte) (int, error) {
	// First checking whether we can satisfy request from buffered file fragments
	var nr int
	if fhr.Buffer1.ReadFromBuffer(fileOffset, buf, &nr) || fhr.Buffer2.ReadFromBuffer(fileOffset, buf, &nr) {
		fhr.CacheHits++
		return nr, nil
	}

	// None of the buffers has the data to satisfy the request, we're going to read more data from backend into Buffer1

	// Before doing that, swapping buffers to keep MRU/LRU invariant
	fhr.Buffer2, fhr.Buffer1 = fhr.Buffer1, fhr.Buffer2

	maxBytesToRead := len(buf)
	minBytesToRead := 1

	if fileOffset != fhr.Offset {
		// We're reading not from the offset expected by the backend stream
		// we need to decide whether we do Seek(), or read the skipped data (refered as "hole" below)
		if fileOffset > fhr.Offset && fileOffset-fhr.Offset <= int64(BLOCKSIZE*2) {
			holeSize := int(fileOffset - fhr.Offset)
			fhr.Holes++
			maxBytesToRead += holeSize    // we're going to read the "hole"
			minBytesToRead = holeSize + 1 // we need to read at least one byte starting from requested offset
		} else {
			fhr.Seeks++
			err := fhr.HdfsReader.Seek(fileOffset)
			// If seek error happens, return err. Seek to the end of the file is not an error.
			if err != nil && fhr.Offset > fileOffset {
				logger.WithFields(logger.Fields{Operation: Read, Path: handle.File.AbsolutePath(), Error: err}).
					Error("Read partial failed due to failed seek. ", " @offset:", fhr.Offset, " Seek error to", fileOffset)
				return 0, err
			}
			fhr.Offset = fileOffset
		}
	}

	// Ceiling to the nearest BLOCKSIZE
	maxBytesToRead = (maxBytesToRead + BLOCKSIZE - 1) / BLOCKSIZE * BLOCKSIZE

	// Reading from backend into Buffer1
	err := fhr.Buffer1.ReadFromBackend(fhr.HdfsReader, &fhr.Offset, minBytesToRead, maxBytesToRead)
	if err != nil {
		if err == io.EOF {
			logger.WithFields(logger.Fields{Operation: Read, Path: handle.File.AbsolutePath(), Error: err}).Warn("EOF @ ", fhr.Offset)
			return 0, err
		}
		return 0, err
	}
	// Now Buffer1 has the data to satisfy request
	if !fhr.Buffer1.ReadFromBuffer(fileOffset, buf, &nr) {
		return 0, errors.New("INTERNAL ERROR: FileFragment invariant")
	}
	return nr, nil
}

// Closes the reader
func (fhr *FileHandleReader) Close() error {
	if fhr.HdfsReader != nil {
		logger.WithFields(logger.Fields{Operation: Read, Path: fhr.Handle.File.AbsolutePath(), Holes: fhr.Holes, CacheHits: fhr.CacheHits, HardSeeks: fhr.Seeks}).Info("Reader closed")
		fhr.HdfsReader.Close()
		fhr.HdfsReader = nil
	}
	return nil
}
