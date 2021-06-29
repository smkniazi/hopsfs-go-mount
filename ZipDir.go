// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"archive/zip"
	"strings"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	logger "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Encapsulates state and operations for a directory inside a zip file on HDFS file system
type ZipDir struct {
	Attrs            Attrs               // Attributes of the directory
	ZipContainerFile *File               // Zip container file node
	IsRoot           bool                // true if this ZipDir represents archive root
	SubDirs          map[string]*ZipDir  // Sub-directories (immediate children)
	Files            map[string]*ZipFile // Files in this directory
	ReadArchiveLock  sync.Mutex          // Used when reading the archive for root zip node (IsRoot==true)
	zipFile          *zip.File
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*ZipDir)(nil)
var _ fs.HandleReadDirAller = (*ZipDir)(nil)
var _ fs.NodeStringLookuper = (*ZipDir)(nil)

// Creates root dir node for zip archive
func NewZipRootDir(zipContainerFile *File, attrs Attrs) *ZipDir {
	return &ZipDir{
		IsRoot:           true,
		ZipContainerFile: zipContainerFile,
		Attrs:            attrs}
}

// Responds on FUSE request to get directory attributes
func (zd *ZipDir) Attr(ctx context.Context, a *fuse.Attr) error {
	return zd.Attrs.Attr(a)
}

// Reads a zip file (once) and pre-creates all the directory/file structure in memory
// This happens under lock. Upen exit from a lock the resulting directory/file structure
// is immutable and safe to access from multiple threads.
func (zd *ZipDir) ReadArchive() error {
	if zd.SubDirs != nil {
		// Archive nodes have been already pre-created, nothing to do
		return nil
	}
	zd.ReadArchiveLock.Lock()
	defer zd.ReadArchiveLock.Unlock()
	// Repeating the check after taking a lock
	if zd.SubDirs != nil {
		// Archive nodes have been already pre-created, nothing to do
		return nil
	}

	// Opening zip file (reading metadata of all archived files)
	randomAccessReader := NewRandomAccessReader(zd.ZipContainerFile)
	var attr fuse.Attr
	err := zd.ZipContainerFile.Attr(nil, &attr)
	if err != nil {
		logger.WithFields(logger.Fields{Operation: ReadArch, Archive: zd.ZipContainerFile.AbsolutePath(), Error: err}).Error("Error getting attrs")
		return err
	}
	zipArchiveReader, err := zip.NewReader(randomAccessReader, int64(attr.Size))
	if err == nil {
		logger.WithFields(logger.Fields{Operation: ReadArch, Archive: zd.ZipContainerFile.AbsolutePath()}).Info("Opened zip file")
	} else {
		logger.WithFields(logger.Fields{Operation: ReadArch, Archive: zd.ZipContainerFile.AbsolutePath(), Error: err}).Error("Error opening zip file")
		return err
	}

	// Register reader to be closed during unmount
	zd.ZipContainerFile.FileSystem.CloseOnUnmount(randomAccessReader)

	zd.SubDirs = make(map[string]*ZipDir)
	zd.Files = make(map[string]*ZipFile)

	// Enumerating all files inside zip archive and pre-creating a tree of ZipDir and ZipFile structures
	for _, zipFile := range zipArchiveReader.File {
		dir := zd
		attrs := Attrs{
			Mode:   zipFile.Mode() | 0700, // Cast the permission to RWX for owner
			Mtime:  zipFile.ModTime(),
			Uid:    zd.Attrs.Uid,
			Gid:    zd.Attrs.Gid,
			Ctime:  zipFile.ModTime(),
			Crtime: zipFile.ModTime(),
			Size:   zipFile.UncompressedSize64,
		}
		// Split path to components
		components := strings.Split(zipFile.Name, "/")
		// Enumerate path components from left to right, creating ZipDir tree as we go
		for i, name := range components {
			if name == "" {
				continue
			}
			attrs.Name = name
			if subDir, ok := dir.SubDirs[name]; ok {
				// Going inside subDir
				dir = subDir
			} else {
				if i == len(components)-1 {
					// Current path component is the last component of the path:
					// Creating ZipFile
					dir.Files[name] = &ZipFile{
						FileSystem: zd.ZipContainerFile.FileSystem,
						zipFile:    zipFile,
						Attrs:      attrs}
				} else {
					// Current path component is a directory, which we haven't previously observed
					// Creating ZipDir
					dir.SubDirs[name] = &ZipDir{
						zipFile:          zipFile,
						ZipContainerFile: zd.ZipContainerFile,
						IsRoot:           false,
						SubDirs:          make(map[string]*ZipDir),
						Files:            make(map[string]*ZipFile),
						Attrs:            attrs}
				}
			}
		}
	}
	return nil
}

// Responds on FUSE request to list directory contents
func (zd *ZipDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	err := zd.ReadArchive()
	if err != nil {
		return nil, err
	}

	entries := make([]fuse.Dirent, 0, len(zd.SubDirs)+len(zd.Files))
	// Creating Dirent structures as required by FUSE for subdirs and files
	for name, _ := range zd.SubDirs {
		entries = append(entries, fuse.Dirent{Name: name, Type: fuse.DT_Dir})
	}
	for name, _ := range zd.Files {
		entries = append(entries, fuse.Dirent{Name: name, Type: fuse.DT_File})
	}
	return entries, nil
}

// Responds on FUSE request to lookup the directory
func (zd *ZipDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	// Responds on FUSE request to Looks up a file or directory by name
	err := zd.ReadArchive()
	if err != nil {
		return nil, err
	}

	if subDir, ok := zd.SubDirs[name]; ok {
		return subDir, nil
	}

	if file, ok := zd.Files[name]; ok {
		return file, nil
	}

	return nil, fuse.ENOENT
}
