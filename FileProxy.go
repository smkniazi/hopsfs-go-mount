// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

type FileProxy interface {
	Truncate(size int64) (int64, error)
	WriteAt(b []byte, off int64) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	SeekToStart() (err error)
	Read(b []byte) (n int, err error)
	Close() error
	Sync() error
}
