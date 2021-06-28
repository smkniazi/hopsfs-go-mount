// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type File struct {
	FileSystem *FileSystem // pointer to the FieSystem which owns this file
	Attrs      Attrs       // Cache of file attributes // TODO: implement TTL
	Parent     *Dir        // Pointer to the parent directory (allows computing fully-qualified paths on demand)

	activeHandles      []*FileHandle // list of opened file handles
	activeHandlesMutex sync.Mutex    // mutex for activeHandles
	tmpFile            string        // temporary copy of the file
}

// Verify that *File implements necesary FUSE interfaces
var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)
var _ fs.NodeFsyncer = (*File)(nil)
var _ fs.NodeSetattrer = (*File)(nil)

// File is also a factory for ReadSeekCloser objects
var _ ReadSeekCloserFactory = (*File)(nil)

// Retuns absolute path of the file in HDFS namespace
func (file *File) AbsolutePath() string {
	return path.Join(file.Parent.AbsolutePath(), file.Attrs.Name)
}

// Responds to the FUSE file attribute request
func (file *File) Attr(ctx context.Context, a *fuse.Attr) error {
	if file.FileSystem.Clock.Now().After(file.Attrs.Expires) {
		err := file.Parent.LookupAttrs(file.Attrs.Name, &file.Attrs)
		if err != nil {
			return err
		}
	}
	return file.Attrs.Attr(a)
}

// Responds to the FUSE file open request (creates new file handle)
func (file *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	file.activeHandlesMutex.Lock()
	defer file.activeHandlesMutex.Unlock()

	Info.Printf("Open: %s, Flags %v", file.AbsolutePath(), req.Flags)
	handle, err := NewFileHandle(file, true, req.Flags)
	if err != nil {
		return nil, err
	}

	file.AddHandle(handle)
	return handle, nil
}

// Opens file for reading
func (file *File) OpenRead() (ReadSeekCloser, error) {
	Error.Panic("Unsupported operation")
	os.Exit(1)

	return nil, nil
	//	handle, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, nil)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return NewFileHandleAsReadSeekCloser(handle.(*FileHandle)), nil
}

// Registers an opened file handle
func (file *File) AddHandle(handle *FileHandle) {
	file.activeHandles = append(file.activeHandles, handle)
}

// Unregisters an opened file handle
func (file *File) RemoveHandle(handle *FileHandle) {
	for i, h := range file.activeHandles {
		if h == handle {
			file.activeHandles = append(file.activeHandles[:i], file.activeHandles[i+1:]...)
			break
		}
	}
}

// Returns a snapshot of opened file handles
func (file *File) GetActiveHandles() []*FileHandle {
	file.activeHandlesMutex.Lock()
	defer file.activeHandlesMutex.Unlock()

	snapshot := make([]*FileHandle, len(file.activeHandles))
	copy(snapshot, file.activeHandles)
	return snapshot
}

// Responds to the FUSE Fsync request
func (file *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	Info.Println("Dispatching fsync request to open handles: ", len(file.GetActiveHandles()))
	var retErr error
	for _, handle := range file.GetActiveHandles() {
		err := handle.Fsync(ctx, req)
		if err != nil {
			retErr = err
		}
	}
	return retErr
}

// Invalidates metadata cache, so next ls or stat gives up-to-date file attributes
func (file *File) InvalidateMetadataCache() {
	file.Attrs.Expires = file.FileSystem.Clock.Now().Add(-1 * time.Second)
}

// Responds on FUSE Chmod request
func (file *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Get the filepath, so chmod in hdfs can work
	path := file.AbsolutePath()
	var err error

	if req.Valid.Mode() {
		Info.Println("Chmod [", path, "] to [", req.Mode, "]")
		(func() {
			err = file.FileSystem.HdfsAccessor.Chmod(path, req.Mode)
			if err != nil {
				return
			}
		})()

		if err != nil {
			Error.Println("Chmod failed with error: ", err)
		} else {
			file.Attrs.Mode = req.Mode
		}
	}

	if req.Valid.Uid() {
		u, err := user.LookupId(fmt.Sprint(req.Uid))
		owner := fmt.Sprint(req.Uid)
		group := fmt.Sprint(req.Gid)
		if err != nil {
			Error.Println("Chown: username for uid", req.Uid, "not found, use uid/gid instead")
		} else {
			owner = u.Username
			group = owner // hardcoded the group same as owner
		}

		Info.Println("Chown [", path, "] to [", owner, ":", group, "]")
		(func() {
			err = file.FileSystem.HdfsAccessor.Chown(path, fmt.Sprint(req.Uid), fmt.Sprint(req.Gid))
			if err != nil {
				return
			}
		})()

		if err != nil {
			Error.Println("Chown failed with error:", err)
		} else {
			file.Attrs.Uid = req.Uid
			file.Attrs.Gid = req.Gid
		}
	}

	return err
}
