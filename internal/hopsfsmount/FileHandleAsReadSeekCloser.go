// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"bazil.org/fuse"
)

// Wraps FileHandle exposing it as ReadSeekCloser intrface
// Concurrency: not thread safe: at most on request at a time
type FileHandleAsReadSeekCloser struct {
	FileHandle *FileHandle
	Offset     int64
}

// Verify that *FileHandleAsReadSeekCloser implements ReadSeekCloser
var _ ReadSeekCloser = (*FileHandleAsReadSeekCloser)(nil)

// Creates new adapter
func NewFileHandleAsReadSeekCloser(fileHandle *FileHandle) ReadSeekCloser {
	return &FileHandleAsReadSeekCloser{FileHandle: fileHandle}
}

// Reads a chunk of data
func (fhrs *FileHandleAsReadSeekCloser) Read(buffer []byte) (int, error) {
	resp := fuse.ReadResponse{Data: buffer}
	err := fhrs.FileHandle.Read(nil, &fuse.ReadRequest{Offset: fhrs.Offset, Size: len(buffer)}, &resp)
	fhrs.Offset += int64(len(resp.Data))
	return len(resp.Data), err
}

// Seeks to a given position
func (fhrs *FileHandleAsReadSeekCloser) Seek(pos int64) error {
	// Note: seek is implemented as virtual operation, error checking will happen
	// when a Read() is called after a problematic Seek()
	fhrs.Offset = pos
	return nil
}

// Returns reading position
func (fhrs *FileHandleAsReadSeekCloser) Position() (int64, error) {
	return fhrs.Offset, nil
}

// Closes the underlying file handle
func (fhrs *FileHandleAsReadSeekCloser) Close() error {
	return fhrs.FileHandle.Release(nil, nil)
}
