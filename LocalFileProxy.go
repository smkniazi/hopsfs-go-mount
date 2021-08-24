package main

import "os"

type LocalFileProxy struct {
	localFile *os.File // handle to the temp file in staging dir
}

var _ FileProxy = (*LocalFileProxy)(nil)

func (p *LocalFileProxy) Truncate(size int64) error {
	return p.localFile.Truncate(size)
}

func (p *LocalFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	return p.localFile.WriteAt(b, off)
}

func (p *LocalFileProxy) ReadAt(b []byte, off int64) (n int, err error) {
	return p.localFile.ReadAt(b, off)
}

func (p *LocalFileProxy) Seek(offset int64, whence int) (ret int64, err error) {
	return p.localFile.Seek(offset, whence)
}

func (p *LocalFileProxy) Read(b []byte) (n int, err error) {
	return p.localFile.Read(b)
}

func (p *LocalFileProxy) Close() error {
	return p.localFile.Close()
}

func (p *LocalFileProxy) Sync() error {
	return p.localFile.Sync()
}
