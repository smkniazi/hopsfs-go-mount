// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

//import (
//	"sync"
//
//	"bazil.org/fuse"
//	"bazil.org/fuse/fs"
//	"golang.org/x/net/context"
//)
//
//// Represends a handle to an open file
//type FileHandle struct {
//	File   *File
//	Reader *FileHandleReader
//	Writer *FileHandleWriter
//	Mutex  sync.Mutex // all operations on the handle are serialized to simplify invariants
//}
//
//// Verify that *FileHandle implements necesary FUSE interfaces
//var _ fs.Node = (*FileHandle)(nil)
//var _ fs.HandleReader = (*FileHandle)(nil)
//var _ fs.HandleReleaser = (*FileHandle)(nil)
//var _ fs.HandleWriter = (*FileHandle)(nil)
//var _ fs.NodeFsyncer = (*FileHandle)(nil)
//var _ fs.HandleFlusher = (*FileHandle)(nil)
//
//// Creates new file handle
//func NewFileHandle(file *File) *FileHandle {
//	return &FileHandle{File: file}
//}
//
//// Opens handle for read mode
//func (fh *FileHandle) EnableRead() error {
//	if fh.Reader != nil {
//		return nil
//	}
//	reader, err := NewFileHandleReader(fh)
//	if err != nil {
//		return err
//	}
//	fh.Reader = reader
//	return nil
//}
//
//// Opens handle for write mode
//func (fh *FileHandle) EnableWrite(newFile bool) error {
//	if fh.Writer != nil {
//		return nil
//	}
//	writer, err := NewFileHandleWriter(fh, newFile)
//	if err != nil {
//		return err
//	}
//	fh.Writer = writer
//	return nil
//}
//
//// Returns attributes of the file associated with this handle
//func (fh *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
//	fh.Mutex.Lock()
//	defer fh.Mutex.Unlock()
//	return fh.File.Attr(ctx, a)
//}
//
//// Responds to FUSE Read request
//func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
//	fh.Mutex.Lock()
//	defer fh.Mutex.Unlock()
//
//	if fh.Reader == nil {
//		Warning.Println("[", fh.File.AbsolutePath(), "] reading file opened for write @", req.Offset)
//		err := fh.EnableRead()
//		if err != nil {
//			return err
//		}
//	}
//
//	return fh.Reader.Read(fh, ctx, req, resp)
//}
//
//// Responds to FUSE Write request
//func (fh *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
//	fh.Mutex.Lock()
//	defer fh.Mutex.Unlock()
//	if fh.Writer == nil {
//		err := fh.EnableWrite(false)
//		if err != nil {
//			return err
//		}
//	}
//	return fh.Writer.Write(fh, ctx, req, resp)
//}
//
//// Responds to the FUSE Flush request
//func (fh *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
//	fh.Mutex.Lock()
//	defer fh.Mutex.Unlock()
//	if fh.Writer != nil {
//		return fh.Writer.Flush()
//	}
//	return nil
//}
//
//// Responds to the FUSE Fsync request
//func (fh *FileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
//	fh.Mutex.Lock()
//	defer fh.Mutex.Unlock()
//	if fh.Writer != nil {
//		return fh.Writer.Flush()
//	}
//	return nil
//}
//
//// Closes the handle
//func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
//	fh.Mutex.Lock()
//	defer fh.Mutex.Unlock()
//	if fh.Reader != nil {
//		err := fh.Reader.Close()
//		Info.Println("[", fh.File.AbsolutePath(), "] Close/Read: err=", err)
//		fh.Reader = nil
//	}
//	if fh.Writer != nil {
//		err := fh.Writer.Close()
//		Info.Println("[", fh.File.AbsolutePath(), "] Close/Write: err=", err)
//		fh.Writer = nil
//	}
//	fh.File.InvalidateMetadataCache()
//	fh.File.RemoveHandle(fh)
//	return nil
//}
//
