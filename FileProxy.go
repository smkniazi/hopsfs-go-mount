package main

type FileProxy interface {
	Truncate(size int64) error
	WriteAt(b []byte, off int64) (n int, err error)
	ReadAt(b []byte, off int64) (n int, err error)
	Seek(offset int64, whence int) (ret int64, err error)
	Read(b []byte) (n int, err error)
	Close() error
	Sync() error
}
