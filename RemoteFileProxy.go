package main

import (
	"errors"
	"os"
)

type RemoteROFileProxy struct {
	hdfsReader ReadSeekCloser
	file       *FileINode
}

var _ FileProxy = (*RemoteROFileProxy)(nil)

func (p *RemoteROFileProxy) Truncate(size int64) error {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	logfatal("Truncate API is not supported. Read only mode", nil)
	return nil
}

func (p *RemoteROFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	logfatal("WriteAt API is not supported. Read only mode", nil)
	return 0, nil
}

func (p *RemoteROFileProxy) ReadAt(b []byte, off int64) (int, error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()

	if off < 0 {
		return 0, &os.PathError{Op: "readat", Path: p.file.AbsolutePath(), Err: errors.New("negative offset")}
	}

	if err := p.hdfsReader.Seek(off); err != nil {
		return 0, err
	}

	var err error = nil
	var n int = 0
	for len(b) > 0 {
		m, e := p.hdfsReader.Read(b)
		if e != nil {
			err = e
			break
		}
		n += m
		b = b[m:]
	}

	logdebug("RemoteFileProxy ReadAt", p.file.logInfo(Fields{Operation: Read, Bytes: n, Error: err, Offset: off}))
	return n, err
}

func (p *RemoteROFileProxy) SeekToStart() (err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	return p.hdfsReader.Seek(0)
}

func (p *RemoteROFileProxy) Read(b []byte) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	return p.hdfsReader.Read(b)
}

func (p *RemoteROFileProxy) Close() error {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	return p.hdfsReader.Close()
}

func (p *RemoteROFileProxy) Sync() error {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	logfatal("Sync API is not supported. Read only mode", nil)
	return nil
}
