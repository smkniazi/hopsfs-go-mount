// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"

	"github.com/colinmarc/hdfs/v2"
)

// Allows to open an HDFS file as a seekable read-only stream
// Concurrency: not thread safe: at most on request at a time
type HdfsReader struct {
	BackendReader *hdfs.FileReader
}

var _ ReadSeekCloser = (*HdfsReader)(nil) // ensure HdfsReader implements ReadSeekCloser

// Creates new instance of HdfsReader
func NewHdfsReader(backendReader *hdfs.FileReader) ReadSeekCloser {
	return &HdfsReader{BackendReader: backendReader}
}

// Read a chunk of data
func (hr *HdfsReader) Read(buffer []byte) (int, error) {
	return hr.BackendReader.Read(buffer)
}

// Seeks to a given position
func (hr *HdfsReader) Seek(pos int64) error {
	actualPos, err := hr.BackendReader.Seek(pos, 0)
	if err != nil {
		return err
	}
	if pos != actualPos {
		return errors.New("Can't seek to requested position")
	}
	return nil
}

// Returns current position
func (hr *HdfsReader) Position() (int64, error) {
	actualPos, err := hr.BackendReader.Seek(0, 1)
	if err != nil {
		return 0, err
	}
	return actualPos, nil
}

// Closes the stream
func (hr *HdfsReader) Close() error {
	return hr.BackendReader.Close()
}
