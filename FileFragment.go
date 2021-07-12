// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

// Represents a buffered (or cached) sequential fragment of the file.
// This could be data read from the file, or a write buffer
type FileFragment struct {
	Offset int64  // offset from the beginning of the file
	Data   []byte // data
}

// Reads into the file fragment buffer from the backend
func (ff *FileFragment) ReadFromBackend(hdfsReader ReadSeekCloser, offset *int64, minBytesToRead int, maxBytesToRead int) error {
	if cap(ff.Data) < maxBytesToRead {
		// not enough capacity - realloating
		ff.Data = make([]byte, maxBytesToRead)
	} else {
		// enough capacity, no realloation
		ff.Data = ff.Data[0:maxBytesToRead]
	}
	totalRead := 0
	ff.Offset = *offset
	var err error = nil
	var nr int
	for totalRead < minBytesToRead {
		nr, err = hdfsReader.Read(ff.Data[totalRead:maxBytesToRead])
		if err != nil {
			break
		}
		*offset += int64(nr)
		totalRead += nr
	}
	ff.Data = ff.Data[0:totalRead]
	return err
}

// Attempts to satisfy a read request using buffered data, returns true if successful
func (ff *FileFragment) ReadFromBuffer(fileOffset int64, buf []byte, nr *int) bool {
	// computing a [start,end) range within this frament
	start := fileOffset - ff.Offset
	if start < 0 || start >= int64(len(ff.Data)) {
		*nr = 0
		return false
	}

	end := start + int64(len(buf))
	if end > int64(len(ff.Data)) {
		end = int64(len(ff.Data))
	}
	copy(buf, ff.Data[start:end])
	*nr = int(end - start)
	return true
}
