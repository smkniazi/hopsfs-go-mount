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
type DirINode struct {
	FileSystem *FileSystem         // Pointer to the owning filesystem
	Attrs      Attrs               // Cached attributes of the directory, TODO: add TTL
	Parent     *DirINode           // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	Entries    map[string]*fs.Node // Cahed directory entries
	mutex      sync.Mutex          // One read or write operation on a directory at a time
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*DirINode)(nil)
var _ fs.HandleReadDirAller = (*DirINode)(nil)
var _ fs.NodeStringLookuper = (*DirINode)(nil)
var _ fs.NodeMkdirer = (*DirINode)(nil)
var _ fs.NodeRemover = (*DirINode)(nil)
var _ fs.NodeRenamer = (*DirINode)(nil)

// Returns absolute path of the dir in HDFS namespace
func (dir *DirINode) AbsolutePath() string {
	if dir.Parent == nil {
		return dir.FileSystem.SrcDir
	} else {
		return path.Join(dir.Parent.AbsolutePath(), dir.Attrs.Name)
	}
}

// Returns absolute path of the child item of this directory
func (dir *DirINode) AbsolutePathForChild(name string) string {
	path := dir.AbsolutePath()
	if path != "/" {
		path = path + "/"
	}
	return path + name
}

// Responds on FUSE request to get directory attributes
func (dir *DirINode) Attr(ctx context.Context, a *fuse.Attr) error {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()
	if dir.Parent != nil && dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
		err := dir.Parent.LookupAttrs(dir.Attrs.Name, &dir.Attrs)
		if err != nil {
			return err
		}

	}
	return dir.Attrs.Attr(a)
}

func (dir *DirINode) EntriesGet(name string) *fs.Node {
	if dir.Entries == nil {
		dir.Entries = make(map[string]*fs.Node)
		return nil
	}
	return dir.Entries[name]
}

func (dir *DirINode) EntriesSet(name string, node *fs.Node) {
	if dir.Entries == nil {
		dir.Entries = make(map[string]*fs.Node)
	}

	dir.Entries[name] = node
}

func (dir *DirINode) EntriesUpdate(name string, attr Attrs) {
	if dir.Entries == nil {
		dir.Entries = make(map[string]*fs.Node)
	}

	if node, ok := dir.Entries[name]; ok {
		if fnode, ok := (*node).(*FileINode); ok {
			fnode.Attrs = attr
		} else if dnode, ok := (*node).(*DirINode); ok {
			dnode.Attrs = attr
		}
	}
}

func (dir *DirINode) EntriesRemove(name string) {
	if dir.Entries != nil {
		delete(dir.Entries, name)
	}
}

// Responds on FUSE request to lookup the directory
func (dir *DirINode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

	if !dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(name)) {
		return nil, fuse.ENOENT
	}

	if node := dir.EntriesGet(name); node != nil {
		return *node, nil
	}

	if dir.FileSystem.ExpandZips && strings.HasSuffix(name, ".zip@") {
		// looking up original zip file
		zipFileName := name[:len(name)-1]
		zipFileNode, err := dir.Lookup(nil, zipFileName)
		if err != nil {
			return nil, err
		}
		zipFile, ok := zipFileNode.(*FileINode)
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
func (dir *DirINode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

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
func (dir *DirINode) NodeFromAttrs(attrs Attrs) fs.Node {
	var node fs.Node
	if (attrs.Mode & os.ModeDir) == 0 {
		node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
	} else {
		node = &DirINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
	}

	if n := dir.EntriesGet(attrs.Name); n != nil {
		dir.EntriesUpdate(attrs.Name, attrs)
	} else {
		dir.EntriesSet(attrs.Name, &node)
	}

	return node
}

// Performs Stat() query on the backend
func (dir *DirINode) LookupAttrs(name string, attrs *Attrs) error {

	var err error
	*attrs, err = dir.FileSystem.HdfsAccessor.Stat(path.Join(dir.AbsolutePath(), name))
	if err != nil {
		// It is a warning as each time new file write tries to stat if the file exists
		loginfo("Stat failed", Fields{Operation: Stat, Path: path.Join(dir.AbsolutePath(), name), Error: err})
		return err
	}

	logdebug("Stat successful ", Fields{Operation: Stat, Path: path.Join(dir.AbsolutePath(), name)})
	// expiration time := now + 5 secs // TODO: make configurable
	attrs.Expires = dir.FileSystem.Clock.Now().Add(5 * time.Second)
	return nil
}

// Responds on FUSE Mkdir request
func (dir *DirINode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

	err := dir.FileSystem.HdfsAccessor.Mkdir(dir.AbsolutePathForChild(req.Name), req.Mode)
	if err != nil {
		return nil, err
	}
	return dir.NodeFromAttrs(Attrs{Name: req.Name, Mode: req.Mode | os.ModeDir}), nil
}

// Responds on FUSE Create request
func (dir *DirINode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

	loginfo("Creating a new file", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags})
	file := dir.NodeFromAttrs(Attrs{Name: req.Name, Mode: req.Mode}).(*FileINode)
	handle, err := NewFileHandle(file, false, req.Flags)
	if err != nil {
		logerror("File creation failed", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags, Error: err})
		//TODO remove the entry from the cache
		return nil, nil, err
	}
	file.AddHandle(handle)
	return file, handle, nil
}

// Responds on FUSE Remove request
func (dir *DirINode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

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
func (dir *DirINode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

	oldPath := dir.AbsolutePathForChild(req.OldName)
	newPath := newDir.(*DirINode).AbsolutePathForChild(req.NewName)
	loginfo("Renaming to "+newPath, Fields{Operation: Rename, Path: oldPath})
	err := dir.FileSystem.HdfsAccessor.Rename(oldPath, newPath)
	if err == nil {
		// Upon successful rename, updating in-memory representation of the file entry
		if node := dir.EntriesGet(req.OldName); node != nil {
			if fnode, ok := (*node).(*FileINode); ok {
				fnode.Attrs.Name = req.NewName
			} else if dnode, ok := (*node).(*DirINode); ok {
				dnode.Attrs.Name = req.NewName
			}
			dir.EntriesRemove(req.OldName)
			newDir.(*DirINode).EntriesSet(req.NewName, node)
		}
	}
	return err
}

// Responds on FUSE Chmod request
func (dir *DirINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	dir.mutex.Lock()
	defer dir.mutex.Unlock()

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
