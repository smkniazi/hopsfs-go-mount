// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"io"
	"sync"
)

// RandomAccessReader implments io.ReaderAt, io.Closer providing efficient concurrent
// random access to the HDFS file. Concurrency is achieved by pooling ReadSeekCloser objects.
// In order to optimize sequential read scenario of a fragment of the file, pool data structure
// is organized as a map keyed by the seek position, so sequential read of adjacent file chunks
// with high probability goes to the same ReadSeekCloser
type RandomAccessReader interface {
	io.ReaderAt
	io.Closer
}

type randomAccessReaderImpl struct {
	File       ReadSeekCloserFactory    // Interface to open a file
	Pool       map[int64]ReadSeekCloser // Pool of ReadSeekCloser objects keyed by the seek position
	PoolLock   sync.Mutex               // Exclusive lock for the Pool
	MaxReaders int                      // Maximum number of readers in the pool
}

var _ RandomAccessReader = (*randomAccessReaderImpl)(nil) // ensure randomAccessReadSeekCloser implements RandomAccessReader

func NewRandomAccessReader(file ReadSeekCloserFactory) RandomAccessReader {
	this := &randomAccessReaderImpl{
		File:       file,
		Pool:       map[int64]ReadSeekCloser{},
		MaxReaders: 256} //TODO: [CR: alexeyk] make configurable
	return this
}

func (rar *randomAccessReaderImpl) ReadAt(buffer []byte, offset int64) (int, error) {
	reader, err := rar.getReaderFromPoolOrCreateNew(offset)
	defer func() {
		if err == nil {
			rar.returnReaderToPool(reader)
		} else {
			if reader != nil {
				go reader.Close()
			}
		}
	}()
	if err != nil {
		return 0, err
	}
	readerPos, err := reader.Position()
	if err != nil {
		return 0, err
	}
	if readerPos != offset {
		err := reader.Seek(offset)
		if err != nil {
			return 0, err
		}
	}
	nr, err := io.ReadFull(reader, buffer)
	return nr, err
}

// Closes all the readers
func (rar *randomAccessReaderImpl) Close() error {
	rar.PoolLock.Lock()
	defer rar.PoolLock.Unlock()
	for _, reader := range rar.Pool {
		reader.Close()
	}
	rar.Pool = nil
	return nil
}

// Retrieves an optimal reader from pool or creates new one
func (rar *randomAccessReaderImpl) getReaderFromPoolOrCreateNew(offset int64) (ReadSeekCloser, error) {
	reader, err := rar.getReaderFromPool(offset)
	if err != nil {
		return reader, err
	}
	if reader != nil {
		return reader, nil
	} else {
		// Opening new file handle
		return rar.File.OpenRead()
	}
}

// Retrieves an optimal reader from pool or nil if pool is empty
func (rar *randomAccessReaderImpl) getReaderFromPool(offset int64) (ReadSeekCloser, error) {
	rar.PoolLock.Lock()
	defer rar.PoolLock.Unlock()
	if rar.Pool == nil {
		return nil, errors.New("RandomAccessReader closed")
	}
	if len(rar.Pool) == 0 {
		// Empty pool
		return nil, nil
	}
	reader, ok := rar.Pool[offset]
	var key int64
	if ok {
		// Found perfect reader
		key = offset
	} else {
		// Take a random reader from the map
		// Note: go randomizes map enumeration, so we're leveraging it here
		for k, v := range rar.Pool {
			key = k
			reader = v
			break
		}
	}
	// removing from pool before returning
	delete(rar.Pool, key)
	return reader, nil
}

// Returns idle reader back to the pool
func (rar *randomAccessReaderImpl) returnReaderToPool(reader ReadSeekCloser) {
	rar.PoolLock.Lock()
	defer rar.PoolLock.Unlock()
	// If pool was destroyed or is full then closing current reader w/o returning
	if rar.Pool == nil || len(rar.Pool) >= rar.MaxReaders {
		go reader.Close()
		return
	}

	// Getting reader position, if failed - we can't return reader to the pool
	key, err := reader.Position()
	if err != nil {
		go reader.Close()
		return
	}

	prevReader, ok := rar.Pool[key]
	if ok {
		// We had other reader at the same position,
		// closing that one
		go prevReader.Close()
	}

	// Returning reader to the pool
	rar.Pool[key] = reader
}
