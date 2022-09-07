package main

import (
	"math"
	"os"
)

type LocalRWFileProxy struct {
	localFile *os.File // handle to the temp file in staging dir
	file      *FileINode
}

var _ FileProxy = (*LocalRWFileProxy)(nil)

func (p *LocalRWFileProxy) Truncate(size int64) (int64, error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()

	statBefore, err := p.localFile.Stat()
	if err != nil {
		return 0, err
	}

	err = p.localFile.Truncate(size)
	if err != nil {
		return 0, err
	}

	statAfter, err := p.localFile.Stat()
	if err != nil {
		return 0, err
	}

	return int64(math.Abs(float64(statAfter.Size()) - float64(statBefore.Size()))), nil
}

func (p *LocalRWFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	return p.localFile.WriteAt(b, off)
}

func (p *LocalRWFileProxy) ReadAt(b []byte, off int64) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	n, err = p.localFile.ReadAt(b, off)
	logdebug("LocalFileProxy ReadAt", p.file.logInfo(Fields{Operation: Read, Bytes: n, Error: err, Offset: off}))
	return
}

func (p *LocalRWFileProxy) SeekToStart() (err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	_, err = p.localFile.Seek(0, 0)
	return
}

func (p *LocalRWFileProxy) Read(b []byte) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	return p.localFile.Read(b)
}

func (p *LocalRWFileProxy) Close() error {
	//NOTE: Locking is done in File.go
	return p.localFile.Close()
}

func (p *LocalRWFileProxy) Sync() error {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	return p.localFile.Sync()
}
