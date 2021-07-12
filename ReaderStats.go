// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"sync/atomic"
)

type ReaderStats struct {
	ReadCount uint64
	SeekCount uint64
}

func (rs *ReaderStats) IncrementRead() {
	if rs != nil {
		atomic.AddUint64(&rs.ReadCount, 1)
	}
}

func (rs *ReaderStats) IncrementSeek() {
	if rs != nil {
		atomic.AddUint64(&rs.SeekCount, 1)
	}
}
