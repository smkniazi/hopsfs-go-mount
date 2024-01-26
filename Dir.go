// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

// Encapsulates state and operations for directory node on the HDFS file system
type DirINode struct {
	FileSystem    *FileSystem        // Pointer to the owning filesystem
	Attrs         Attrs              // Cached attributes of the directory, TODO: add TTL
	Parent        *DirINode          // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	children      map[string]fs.Node // Cahed directory entries
	childrenMutex sync.Mutex         // for concurrent read and updates
	dirMutex      sync.Mutex         // One read or write operation on a directory at a time
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*DirINode)(nil)
var _ fs.HandleReadDirAller = (*DirINode)(nil)
var _ fs.NodeStringLookuper = (*DirINode)(nil)
var _ fs.NodeMkdirer = (*DirINode)(nil)
var _ fs.NodeRemover = (*DirINode)(nil)
var _ fs.NodeRenamer = (*DirINode)(nil)
var _ fs.NodeForgetter = (*DirINode)(nil)

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
	return path + filepath.Base(name)
}

// Responds on FUSE request to get directory attributes
func (dir *DirINode) Attr(ctx context.Context, a *fuse.Attr) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	if dir.Parent != nil && dir.FileSystem.Clock.Now().After(dir.Attrs.Expires) {
		_, err := dir.Parent.statInodeInHopsFS(GetattrDir, dir.Attrs.Name, &dir.Attrs)
		if err != nil {
			return err
		}
	} else {
		loginfo("Stat successful. Returning from Cache ", Fields{Operation: GetattrDir, Path: path.Join(dir.AbsolutePath(), dir.Attrs.Name), FileSize: dir.Attrs.Size,
			IsDir: dir.Attrs.Mode.IsDir(), IsRegular: dir.Attrs.Mode.IsRegular()})
	}
	return dir.Attrs.ConvertAttrToFuse(a)
}

func (dir *DirINode) getChildInode(operation, name string) fs.Node {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
		return nil
	}

	node := dir.children[name]
	if node != nil {
		logdebug("Children's List. getChildInode ", Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	} else {
		logdebug("Children's List. getChildInode. Not Found  ", Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	}

	return node
}

func (dir *DirINode) addOrUpdateChildInodeAttrs(operation, name string, attrs Attrs) fs.Node {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children == nil {
		dir.children = make(map[string]fs.Node)
	}

	if node, ok := dir.children[name]; ok {
		if fnode, ok := (node).(*FileINode); ok {
			fnode.Attrs = attrs
		} else if dnode, ok := (node).(*DirINode); ok {
			dnode.Attrs = attrs
		}
		logdebug("Children's List. addOrUpdateChildInodeAttrs. Update ", Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
		return node
	} else {
		var node fs.Node
		if (attrs.Mode & os.ModeDir) == 0 {
			node = &FileINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		} else {
			node = &DirINode{FileSystem: dir.FileSystem, Parent: dir, Attrs: attrs}
		}
		dir.children[name] = node
		logdebug("Children's List. addOrUpdateChildInodeAttrs. Add ", Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
		return node
	}
}

func (dir *DirINode) removeChildInode(operation, name string) {
	dir.lockChildrenMutex()
	defer dir.unlockChildrenMutex()

	if dir.children != nil {
		delete(dir.children, name)
		logdebug("Children's List. removeChildInode ", Fields{Operation: operation, Parent: dir.AbsolutePath(), Child: name, NumChildren: len(dir.children)})
	}
}

// Responds on FUSE request to lookup the directory
func (dir *DirINode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	if !dir.FileSystem.IsPathAllowed(dir.AbsolutePathForChild(name)) {
		return nil, syscall.ENOENT
	}

	if node := dir.getChildInode(Lookup, name); node != nil {
		return node, nil
	}

	var attrs Attrs
	node, err := dir.statInodeInHopsFS(Lookup, name, &attrs)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// Responds on FUSE request to read directory
func (dir *DirINode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	absolutePath := dir.AbsolutePath()
	loginfo("Read directory", Fields{Operation: ReadDir, Path: absolutePath})

	allAttrs, err := dir.FileSystem.getDFSConnector().ReadDir(absolutePath)
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
			dir.addOrUpdateChildInodeAttrs(ReadDir, a.Name, a)
		}
	}
	return entries, nil
}

// Performs Stat() query on the backend
func (dir *DirINode) statInodeInHopsFS(operation, name string, attrs *Attrs) (fs.Node, error) {

	a, err := dir.FileSystem.getDFSConnector().Stat(path.Join(dir.AbsolutePath(), name))
	if err != nil {
		loginfo("Stat failed on backend", Fields{Operation: operation, Path: path.Join(dir.AbsolutePath(), name), Error: err})
		dir.removeChildInode(operation, name)
		return nil, err
	}
	*attrs = a

	inode := dir.addOrUpdateChildInodeAttrs(operation, name, *attrs)
	loginfo("Stat successful on backend", Fields{Operation: operation, Path: path.Join(dir.AbsolutePath(), name), FileSize: attrs.Size,
		IsDir: attrs.Mode.IsDir(), IsRegular: attrs.Mode.IsRegular()})
	return inode, nil
}

// Responds on FUSE Mkdir request
func (dir *DirINode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	err := dir.FileSystem.getDFSConnector().Mkdir(dir.AbsolutePathForChild(req.Name), req.Mode)
	if err != nil {
		loginfo("mkdir failed", Fields{Operation: Mkdir, Path: path.Join(dir.AbsolutePath(), req.Name), Error: err})
		return nil, err
	}
	logdebug("mkdir successful", Fields{Operation: Mkdir, Path: path.Join(dir.AbsolutePath(), req.Name)})

	err = ChownOp(&dir.Attrs, dir.FileSystem, dir.AbsolutePathForChild(req.Name), req.Uid, req.Gid)
	if err != nil {
		logwarn("Unable to change ownership of new dir", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name),
			UID: req.Uid, GID: req.Gid, Error: err})
		//unable to change the ownership of the directory. so delete it as the operation as a whole failed
		dir.FileSystem.getDFSConnector().Remove(dir.AbsolutePathForChild(req.Name))
		return nil, err
	}

	newInode := dir.addOrUpdateChildInodeAttrs(Mkdir, req.Name, Attrs{Name: req.Name, Mode: req.Mode | os.ModeDir, Uid: req.Uid, Gid: req.Gid})
	return newInode, nil
}

// Responds on FUSE Create request
func (dir *DirINode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dir.lockMutex()
	defer dir.unlockMutex()

	loginfo("Creating a new file", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags})
	file := (dir.addOrUpdateChildInodeAttrs(Create, req.Name, Attrs{Name: req.Name, Mode: req.Mode})).(*FileINode)
	handle, err := file.NewFileHandle(false, req.Flags)
	if err != nil {
		logerror("File creation failed", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name), Mode: req.Mode, Flags: req.Flags, Error: err})
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

	file.AddHandle(handle)
	err = ChownOp(&dir.Attrs, dir.FileSystem, dir.AbsolutePathForChild(req.Name), req.Uid, req.Gid)
	if err != nil {
		logwarn("Unable to change ownership of new file", Fields{Operation: Create, Path: dir.AbsolutePathForChild(req.Name),
			UID: req.Uid, GID: req.Gid, Error: err})
		//unable to change the ownership of the file. so delete it as the operation as a whole failed
		dir.FileSystem.getDFSConnector().Remove(dir.AbsolutePathForChild(req.Name))
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

	//update the attributes of the file now
	_, err = dir.statInodeInHopsFS(Create, file.Attrs.Name, &file.Attrs)
	if err != nil {
		dir.removeChildInode(Create, req.Name)
		return nil, nil, err
	}

	return file, handle, nil
}

// Responds on FUSE Remove request
func (dir *DirINode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	path := dir.AbsolutePathForChild(req.Name)
	loginfo("Removing path", Fields{Operation: Remove, Path: path})
	err := dir.FileSystem.getDFSConnector().Remove(path)
	if err == nil {
		dir.removeChildInode(Remove, req.Name)
	} else {
		logwarn("Failed to remove path", Fields{Operation: Remove, Path: path, Error: err})
	}
	return err
}

// Responds on FUSE Rename request
func (dir *DirINode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	oldPath := dir.AbsolutePathForChild(req.OldName)
	newPath := newDir.(*DirINode).AbsolutePathForChild(req.NewName)
	loginfo("Renaming to "+newPath, Fields{Operation: Rename, Path: oldPath})
	err := dir.FileSystem.getDFSConnector().Rename(oldPath, newPath)
	if err == nil {
		// Upon successful rename, updating in-memory representation of the file entry
		if node := dir.getChildInode(Rename, req.OldName); node != nil {
			if fnode, ok := (node).(*FileINode); ok {
				fnode.Attrs.Name = req.NewName
				fnode.Parent = newDir.(*DirINode)
				newDir.(*DirINode).addOrUpdateChildInodeAttrs(Rename, req.NewName, fnode.Attrs)
			} else if dnode, ok := (node).(*DirINode); ok {
				dnode.Attrs.Name = req.NewName
				dnode.Parent = newDir.(*DirINode)
				newDir.(*DirINode).addOrUpdateChildInodeAttrs(Rename, req.NewName, dnode.Attrs)
			}
			dir.removeChildInode(Rename, req.OldName)
		}
	}
	return err
}

// Responds on FUSE Chmod request
func (dir *DirINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	dir.lockMutex()
	defer dir.unlockMutex()

	if req.Valid.Size() {
		return fmt.Errorf("unsupported operation. Can not set size of a directory")
	}

	path := dir.AbsolutePath()

	if req.Valid.Mode() {
		if err := ChmodOp(&dir.Attrs, dir.FileSystem, path, req, resp); err != nil {
			logwarn("Setattr (chmod) failed. ", Fields{Operation: Chmod, Path: path, Mode: req.Mode})
			return err
		}
	}

	if req.Valid.Uid() || req.Valid.Gid() {
		if err := SetAttrChownOp(&dir.Attrs, dir.FileSystem, path, req, resp); err != nil {
			logwarn("Setattr (chown/chgrp )failed", Fields{Operation: Chmod, Path: path, UID: req.Uid, GID: req.Gid})
			return err
		}
	}

	if err := UpdateTS(&dir.Attrs, dir.FileSystem, path, req, resp); err != nil {
		return err
	}

	return nil
}

// Responds on FUSE request to forget inode
func (dir *DirINode) Forget() {
	dir.lockMutex()
	defer dir.unlockMutex()
	// ask parent to remove me from the children list
	logdebug(fmt.Sprintf("Forget for dir %s", dir.Attrs.Name), nil)
	dir.Parent.removeChildInode(Forget, dir.Attrs.Name)
}

func (dir *DirINode) lockMutex() {
	dir.dirMutex.Lock()
}

func (dir *DirINode) unlockMutex() {
	dir.dirMutex.Unlock()
}

func (dir *DirINode) lockChildrenMutex() {
	dir.childrenMutex.Lock()
}

func (dir *DirINode) unlockChildrenMutex() {
	dir.childrenMutex.Unlock()
}
