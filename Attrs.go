// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"os"
	"time"

	"bazil.org/fuse"
)

// Attributes common to the file/directory HDFS nodes
type Attrs struct {
	Inode   uint64
	Name    string
	Mode    os.FileMode
	Size    uint64
	Uid     uint32
	Gid     uint32
	Mtime   time.Time
	Ctime   time.Time
	Crtime  time.Time
	Expires time.Time // indicates when cached attribute information expires
}

// FsInfo provides information about HDFS
type FsInfo struct {
	capacity  uint64
	used      uint64
	remaining uint64
}

// Converts Attrs datastructure into FUSE represnetation
func (attrs *Attrs) ConvertAttrToFuse(a *fuse.Attr) error {
	a.Inode = attrs.Inode
	a.Mode = attrs.Mode
	if (a.Mode & os.ModeDir) == 0 {
		a.Size = attrs.Size
	}
	a.Uid = attrs.Uid
	a.Gid = attrs.Gid
	a.Mtime = attrs.Mtime
	a.Ctime = attrs.Ctime
	a.Crtime = attrs.Crtime
	return nil
}

// returns fuse.DirentType for this attributes (DT_Dir or DT_File)
func (attrs *Attrs) FuseNodeType() fuse.DirentType {
	if (attrs.Mode & os.ModeDir) == os.ModeDir {
		return fuse.DT_Dir
	} else {
		return fuse.DT_File
	}
}
