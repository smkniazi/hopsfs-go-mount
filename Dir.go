// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

// Encapsulates state and operations for directory node on the HDFS file system
type Dir struct {
	FileSystem   *FileSystem        // Pointer to the owning filesystem
	Attrs        Attrs              // Cached attributes of the directory, TODO: add TTL
	Parent       *Dir               // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	Entries      map[string]fs.Node // Cahed directory entries
	EntriesMutex sync.Mutex         // Used to protect Entries
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)
var _ fs.NodeMkdirer = (*Dir)(nil)
var _ fs.NodeRemover = (*Dir)(nil)
var _ fs.NodeRenamer = (*Dir)(nil)

// Returns absolute path of the dir in HDFS namespace
func (dir *Dir) AbsolutePath() string {
	if dir.Parent == nil {
		return "/"
	} else {
		return path.Join(dir.Parent.AbsolutePath(), dir.Attrs.Name)
	}
}

// Returns absolute path of the child item of this directory
func (dir *Dir) AbsolutePathForChild(name string) string {
	path := dir.AbsolutePath()
	if path != "/" {
		path = path + "/"
	}
	return path + name
}

// Responds on FUSE request to get directory attributes
func (dir *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	if dir.Parent != nil && dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
		err := dir.Parent.LookupAttrs(dir.Attrs.Name, &dir.Attrs)
		if err != nil {
			return err
		}

	}
	return dir.Attrs.Attr(a)
}

func (dir *Dir) EntriesGet(name string) fs.Node {
	dir.EntriesMutex.Lock()
	defer dir.EntriesMutex.Unlock()
	if dir.Entries == nil {
		dir.Entries = make(map[string]fs.Node)
		return nil
	}
	return dir.Entries[name]
}

func (dir *Dir) EntriesSet(name string, node fs.Node) {
	dir.EntriesMutex.Lock()
	defer dir.EntriesMutex.Unlock()

	if dir.Entries == nil {
		dir.Entries = make(map[string]fs.Node)
	}

	dir.Entries[name] = node
}

func (dir *Dir) EntriesRemove(name string) {
	dir.EntriesMutex.Lock()
	defer dir.EntriesMutex.Unlock()
	if dir.Entries != nil {
		delete(dir.Entries, name)
	}
}

// Responds on FUSE request to lookup the directory
func (dir *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if !dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(name)) {
		return nil, fuse.ENOENT
	}

	if node := dir.EntriesGet(name); node != nil {
		return node, nil
	}

	if dir.FileSystem.ExpandZips && strings.HasSuffix(name, ".zip@") {
		// looking up original zip file
		zipFileName := name[:len(name)-1]
		zipFileNode, err := dir.Lookup(nil, zipFileName)
		if err != nil {
			return nil, err
		}
		zipFile, ok := zipFileNode.(*File)
		if !ok {
			return nil, fuse.ENOENT
		}
		attrs := zipFile.Attrs
		attrs.Mode |= os.ModeDir | 0111 // TODO: set x only if r is set
		attrs.Name = name
		attrs.Inode = 0 // let underlying FUSE layer to assign inodes automatically
		return NewZipRootDir(zipFile, attrs), nil
	}

	var attrs Attrs
	err := dir.LookupAttrs(name, &attrs)
	if err != nil {
		return nil, err
	}
	return dir.NodeFromAttrs(attrs), nil
}

// Responds on FUSE request to read directory
func (dir *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	absolutePath := dir.AbsolutePath()
	loginfo("Read directory", Fields{Operation: ReadDir, Path: absolutePath})

	allAttrs, err := dir.FileSystem.HdfsAccessor.ReadDir(absolutePath)
	if err != nil {
		logwarn("Failed to list DFS directory", Fields{Operation: ReadDir, Path: absolutePath, Error: err})
		return nil, err
	}

	entries := make([]fuse.Dirent, 0, len(allAttrs))
	for _, a := range allAttrs {
		if dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(a.Name)) {
			// Creating Dirent structure as required by FUSE
			entries = append(entries, fuse.Dirent{
				Inode: a.Inode,
				Name:  a.Name,
				Type:  a.FuseNodeType()})
			// Speculatively pre-creating child Dir or File node with cached attributes,
			// since it's highly likely that we will have Lookup() call for this name
			// This is the key trick which dramatically speeds up 'ls'
			dir.NodeFromAttrs(a)

			if dir.FileSystem.ExpandZips {
				// Creating a virtual directory next to each zip file
				// (appending '@' to the zip file name)
				if !a.Mode.IsDir() && strings.HasSuffix(a.Name, ".zip") {
					entries = append(entries, fuse.Dirent{
						Name: a.Name + "@",
						Type: fuse.DT_Dir})
				}
			}
		}
	}
	return entries, nil
}

// Creates typed node (Dir or File) from the attributes
func (dir *Dir) NodeFromAttrs(attrs Attrs) fs.Node {
	var node fs.Node
	if (attrs.Mode & os.ModeDir) == 0 {
		node = &File{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
	} else {
		node = &Dir{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
	}
	dir.EntriesSet(attrs.Name, node)
	return node
}

// Performs Stat() query on the backend
func (dir *Dir) LookupAttrs(name string, attrs *Attrs) error {
	var err error
	*attrs, err = dir.FileSystem.HdfsAccessor.Stat(path.Join(dir.AbsolutePath(), name))
	if err != nil {
		// It is a warning as each time new file write tries to stat if the file exists
		loginfo("Stat failed", Fields{Operation: Stat, Path: path.Join(dir.AbsolutePath(), name), Error: err})
		if pathError, ok := err.(*os.PathError); ok && (pathError.Err == os.ErrNotExist) {
			return fuse.ENOENT
		}
		return err
	}

	logdebug("Stat successful ", Fields{Operation: Stat, Path: path.Join(dir.AbsolutePath(), name)})
	// expiration time := now + 5 secs // TODO: make configurable
	attrs.Expires = dir.FileSystem.Clock.Now().Add(5 * time.Second)
	return nil
}

// Responds on FUSE Mkdir request
func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	err := dir.FileSystem.HdfsAccessor.Mkdir(dir.AbsolutePathForChild(req.Name), req.Mode)
	if err != nil {
		return nil, err
	}
	return dir.NodeFromAttrs(Attrs{Name: req.Name, Mode: req.Mode | os.ModeDir}), nil
}

// Responds on FUSE Create request
func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	loginfo("Creating a new file", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags})

	file := dir.NodeFromAttrs(Attrs{Name: req.Name, Mode: req.Mode}).(*File)
	handle, err := NewFileHandle(file, false, req.Flags)
	if err != nil {
		logerror("File creation failed", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags, Error: err})
		return nil, nil, err
	}
	file.AddHandle(handle)
	return file, handle, nil
}

// Responds on FUSE Remove request
func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	path := dir.AbsolutePathForChild(req.Name)
	loginfo("Removing path", Fields{Operation: Remove, Path: path})
	err := dir.FileSystem.HdfsAccessor.Remove(path)
	if err == nil {
		dir.EntriesRemove(req.Name)
	} else {
		logerror("Failed to remove path", Fields{Operation: Remove, Path: path, Error: err})
	}
	return err
}

// Responds on FUSE Rename request
func (dir *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	oldPath := dir.AbsolutePathForChild(req.OldName)
	newPath := newDir.(*Dir).AbsolutePathForChild(req.NewName)
	loginfo("Renaming to "+newPath, Fields{Operation: Rename, Path: oldPath})
	err := dir.FileSystem.HdfsAccessor.Rename(oldPath, newPath)
	if err == nil {
		// Upon successful rename, updating in-memory representation of the file entry
		if node := dir.EntriesGet(req.OldName); node != nil {
			if fnode, ok := node.(*File); ok {
				fnode.Attrs.Name = req.NewName
			} else if dnode, ok := node.(*Dir); ok {
				dnode.Attrs.Name = req.NewName
			}
			dir.EntriesRemove(req.OldName)
			newDir.(*Dir).EntriesSet(req.NewName, node)
		}
	}
	return err
}

// Responds on FUSE Chmod request
func (dir *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Get the filepath, so chmod in hdfs can work
	path := dir.AbsolutePath()
	var err error

	if req.Valid.Mode() {
		loginfo("Setting attributes", Fields{Operation: Chmod, Path: path, Mode: req.Mode})
		(func() {
			err = dir.FileSystem.HdfsAccessor.Chmod(path, req.Mode)
			if err != nil {
				return
			}
		})()

		if err != nil {
			logerror("Failed to set attributes", Fields{Operation: Chmod, Path: path, Mode: req.Mode, Error: err})
		} else {
			dir.Attrs.Mode = req.Mode
		}
	}

	if req.Valid.Uid() {
		u, err := user.LookupId(fmt.Sprint(req.Uid))
		owner := fmt.Sprint(req.Uid)
		group := fmt.Sprint(req.Gid)
		if err != nil {
			logerror(fmt.Sprintf("Chown: username for uid %d not found, use uid/gid instead", req.Uid),
				Fields{Operation: Chown, Path: path, User: u, UID: owner, GID: group, Error: err})
		} else {
			owner = u.Username
			group = owner // hardcoded the group same as owner until LookupGroupId available
		}

		loginfo("Setting attributes", Fields{Operation: Chown, Path: path, User: u, UID: owner, GID: group})
		(func() {
			err = dir.FileSystem.HdfsAccessor.Chown(path, owner, group)
			if err != nil {
				return
			}
		})()

		if err != nil {
			logerror("Failed to set attributes", Fields{Operation: Chown, Path: path, User: u, UID: owner, GID: group, Error: err})
		} else {
			dir.Attrs.Uid = req.Uid
			dir.Attrs.Gid = req.Gid
		}
	}

	return err
}
