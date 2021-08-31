// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/user"
	"path"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"golang.org/x/sys/unix"
)

type FileINode struct {
	FileSystem *FileSystem // pointer to the FieSystem which owns this file
	Attrs      Attrs       // Cache of file attributes // TODO: implement TTL
	Parent     *DirINode   // Pointer to the parent directory (allows computing fully-qualified paths on demand)

	activeHandles   []*FileHandle // list of opened file handles
	fileMutex       sync.Mutex    // mutex for file operation such as open, delete
	handle          FileProxy     // handle to the temp file in staging dir
	fileHandleMutex sync.Mutex    // mutex for file handle
}

// Verify that *File implements necesary FUSE interfaces
var _ fs.Node = (*FileINode)(nil)
var _ fs.NodeOpener = (*FileINode)(nil)
var _ fs.NodeFsyncer = (*FileINode)(nil)
var _ fs.NodeSetattrer = (*FileINode)(nil)

// File is also a factory for ReadSeekCloser objects
var _ ReadSeekCloserFactory = (*FileINode)(nil)

// Retuns absolute path of the file in HDFS namespace
func (file *FileINode) AbsolutePath() string {
	return path.Join(file.Parent.AbsolutePath(), file.Attrs.Name)
}

// Responds to the FUSE file attribute request
func (file *FileINode) Attr(ctx context.Context, a *fuse.Attr) error {
	file.fileMutex.Lock()
	defer file.fileMutex.Unlock()
	if file.FileSystem.Clock.Now().After(file.Attrs.Expires) {
		err := file.Parent.LookupAttrs(file.Attrs.Name, &file.Attrs)
		if err != nil {
			return err
		}
	}
	return file.Attrs.Attr(a)
}

// Responds to the FUSE file open request (creates new file handle)
func (file *FileINode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	file.fileMutex.Lock()
	defer file.fileMutex.Unlock()

	logdebug("Opening file", Fields{Operation: Open, Path: file.AbsolutePath(), Flags: req.Flags})
	handle, err := file.NewFileHandle(true, req.Flags)
	if err != nil {
		return nil, err
	}

	file.AddHandle(handle)
	return handle, nil
}

// Opens file for reading
func (file *FileINode) OpenRead() (ReadSeekCloser, error) {
	file.fileMutex.Lock()
	defer file.fileMutex.Unlock()
	handle, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, nil)
	if err != nil {
		return nil, err
	}
	return NewFileHandleAsReadSeekCloser(handle.(*FileHandle)), nil
}

// Registers an opened file handle
func (file *FileINode) AddHandle(handle *FileHandle) {
	file.activeHandles = append(file.activeHandles, handle)
}

// Unregisters an opened file handle
func (file *FileINode) RemoveHandle(handle *FileHandle) {
	for i, h := range file.activeHandles {
		if h == handle {
			file.activeHandles = append(file.activeHandles[:i], file.activeHandles[i+1:]...)
			break
		}
	}
}

// Responds to the FUSE Fsync request
func (file *FileINode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	loginfo(fmt.Sprintf("Dispatching fsync request to all open handles: %d", len(file.activeHandles)), Fields{Operation: Fsync})
	file.fileMutex.Lock()
	defer file.fileMutex.Unlock()
	var retErr error
	for _, handle := range file.activeHandles {
		err := handle.Fsync(ctx, req)
		if err != nil {
			retErr = err
		}
	}
	return retErr
}

// Invalidates metadata cache, so next ls or stat gives up-to-date file attributes
func (file *FileINode) InvalidateMetadataCache() {
	file.Attrs.Expires = file.FileSystem.Clock.Now().Add(-1 * time.Second)
}

// Responds on FUSE Chmod request
func (file *FileINode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	file.fileMutex.Lock()
	defer file.fileMutex.Unlock()

	if req.Valid.Size() {
		var retErr error
		for _, handle := range file.activeHandles {
			if handle.dataChanged() { // to only write enabled handles
				err := handle.Truncate(int64(req.Size))
				if err != nil {
					retErr = err
				}
			}
		}
		return retErr
	}

	// Get the filepath, so chmod in hdfs can work
	path := file.AbsolutePath()
	var err error

	if req.Valid.Mode() {
		loginfo("Setting attributes", Fields{Operation: Chmod, Path: path, Mode: req.Mode})
		(func() {
			err = file.FileSystem.getDFSConnector().Chmod(path, req.Mode)
			if err != nil {
				return
			}
		})()

		if err != nil {
			logerror("Failed to set attributes", Fields{Operation: Chmod, Path: path, Mode: req.Mode, Error: err})
		} else {
			file.Attrs.Mode = req.Mode
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
			group = owner // hardcoded the group same as owner
		}

		loginfo("Setting attributes", Fields{Operation: Chown, Path: path, User: u, UID: owner, GID: group})
		(func() {
			err = file.FileSystem.getDFSConnector().Chown(path, fmt.Sprint(req.Uid), fmt.Sprint(req.Gid))
			if err != nil {
				return
			}
		})()

		if err != nil {
			logerror("Failed to set attributes", Fields{Operation: Chown, Path: path, User: u, UID: owner, GID: group, Error: err})
		} else {
			file.Attrs.Uid = req.Uid
			file.Attrs.Gid = req.Gid
		}
	}

	return err
}

func (file *FileINode) countActiveHandles() int {
	return len(file.activeHandles)
}

func (file *FileINode) createStagingFile(operation string, existsInDFS bool) (*os.File, error) {
	if file.handle != nil {
		return nil, nil // there is already an active handle.
	}

	//create staging file
	absPath := file.AbsolutePath()
	hdfsAccessor := file.FileSystem.getDFSConnector()
	if !existsInDFS { // it  is a new file so create it in the DFS
		w, err := hdfsAccessor.CreateFile(absPath, file.Attrs.Mode, false)
		if err != nil {
			logerror("Failed to create file in DFS", file.logInfo(Fields{Operation: operation, Error: err}))
			return nil, err
		}
		loginfo("Created an empty file in DFS", file.logInfo(Fields{Operation: operation}))
		w.Close()
	} else {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(absPath)
		if err != nil {
			logerror("Failed to stat file in DFS", file.logInfo(Fields{Operation: operation, Error: err}))
			return nil, syscall.ENOENT
		}
	}

	stagingFile, err := ioutil.TempFile(stagingDir, "stage")
	if err != nil {
		logerror("Failed to create staging file", file.logInfo(Fields{Operation: operation, Error: err}))
		return nil, err
	}
	os.Remove(stagingFile.Name())
	loginfo("Created staging file", file.logInfo(Fields{Operation: operation, TmpFile: stagingFile.Name()}))

	if existsInDFS {
		if err := file.downloadToStaging(stagingFile, operation); err != nil {
			return nil, err
		}
	}
	return stagingFile, nil
}

func (file *FileINode) downloadToStaging(stagingFile *os.File, operation string) error {
	hdfsAccessor := file.FileSystem.getDFSConnector()
	absPath := file.AbsolutePath()

	reader, err := hdfsAccessor.OpenRead(absPath)
	if err != nil {
		logerror("Failed to open file in DFS", file.logInfo(Fields{Operation: operation, Error: err}))
		// TODO remove the staging file if there are no more active handles
		return err
	}

	nc, err := io.Copy(stagingFile, reader)
	if err != nil {
		logerror("Failed to copy content to staging file", file.logInfo(Fields{Operation: operation, Error: err}))
		return err
	}
	reader.Close()
	loginfo(fmt.Sprintf("Downloaded a copy to stating dir. %d bytes copied", nc), file.logInfo(Fields{Operation: operation}))
	return nil
}

// Creates new file handle
func (file *FileINode) NewFileHandle(existsInDFS bool, flags fuse.OpenFlags) (*FileHandle, error) {
	file.lockFileHandle()
	defer file.unLockFileHandle()

	fh := &FileHandle{File: file, fileFlags: flags, fhID: int64(rand.Uint64())}
	operation := Create
	if existsInDFS {
		operation = Open
	}

	if operation == Create {
		// there must be no existing file handles for create operation
		if file.handle != nil {
			logpanic("Unexpected file state during creation", file.logInfo(Fields{Flags: flags}))
		}
		if err := file.checkDiskSpace(); err != nil {
			return nil, err
		}
		stagingFile, err := file.createStagingFile(operation, existsInDFS)
		if err != nil {
			return nil, err
		}
		fh.File.handle = &LocalRWFileProxy{localFile: stagingFile, file: file}
		loginfo("Opened file, RW handle", fh.logInfo(Fields{Operation: operation, Flags: fh.fileFlags}))
	} else {
		if file.handle != nil {
			fh.File.handle = file.handle
			loginfo("Opened file, Returning existing handle", fh.logInfo(Fields{Operation: operation, Flags: fh.fileFlags}))
		} else {
			// we alway open the file in RO mode. when the client writes to the file
			// then we upgrade the handle. However, if the file is already opened in
			// in RW state then we use the existing RW handle
			// if file.handle
			reader, _ := file.FileSystem.getDFSConnector().OpenRead(file.AbsolutePath())
			fh.File.handle = &RemoteROFileProxy{hdfsReader: reader, file: file}
			loginfo("Opened file, RO handle", fh.logInfo(Fields{Operation: operation, Flags: fh.fileFlags}))
		}
	}
	return fh, nil
}

// changes RO file handle to RW
func (file *FileINode) upgradeHandleForWriting() error {
	file.lockFileHandle()
	defer file.unLockFileHandle()

	var upgrade = false
	if _, ok := file.handle.(*LocalRWFileProxy); ok {
		upgrade = false
	} else if _, ok := file.handle.(*RemoteROFileProxy); ok {
		upgrade = true
	} else {
		logpanic("Unrecognized remote file proxy", nil)
	}

	if !upgrade {
		return nil
	} else {
		remoteROFileProxy, _ := file.handle.(*RemoteROFileProxy)
		remoteROFileProxy.hdfsReader.Close() // close this read only handle
		file.handle = nil

		if err := file.checkDiskSpace(); err != nil {
			return err
		}

		stagingFile, err := file.createStagingFile("Open", true)
		if err != nil {
			return err
		}

		file.handle = &LocalRWFileProxy{localFile: stagingFile, file: file}
		loginfo("Open handle upgrade to support RW ", file.logInfo(Fields{Operation: "Open"}))
		return nil
	}
}

func (file *FileINode) checkDiskSpace() error {
	var stat unix.Statfs_t
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	unix.Statfs(wd, &stat)
	// Available blocks * size per block = available space in bytes
	bytesAvailable := stat.Bavail * uint64(stat.Bsize)
	if bytesAvailable < 64*1024*1024 {
		return syscall.ENOSPC
	} else {
		return nil
	}
}

func (file *FileINode) logInfo(fields Fields) Fields {
	f := Fields{Path: file.AbsolutePath()}
	for k, e := range fields {
		f[k] = e
	}
	return f
}

func (file *FileINode) lockFileHandle() {
	file.fileHandleMutex.Lock()
}

func (file *FileINode) unLockFileHandle() {
	file.fileHandleMutex.Unlock()
}
