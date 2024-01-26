// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"
	"io"
	"os"
)

type RemoteROFileProxy struct {
	hdfsReader ReadSeekCloser
	file       *FileINode
}

var _ FileProxy = (*RemoteROFileProxy)(nil)

func (p *RemoteROFileProxy) Truncate(size int64) (int64, error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	Logfatal("Truncate API is not supported. Read only mode", nil)
	return 0, nil
}

func (p *RemoteROFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	Logfatal("WriteAt API is not supported. Read only mode", nil)
	return 0, nil
}

func (p *RemoteROFileProxy) ReadAt(b []byte, off int64) (int, error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()

	Logdebug("RemoteFileProxy ReadAt", p.file.logInfo(Fields{Operation: Read, Offset: off}))

	if off < 0 {
		return 0, &os.PathError{Op: "readat", Path: p.file.AbsolutePath(), Err: errors.New("negative offset")}
	}
	maxBytesToRead := len(b)

	if err := p.hdfsReader.Seek(off); err != nil {
		return 0, err
	}

	var err error = nil
	var n int = 0
	for len(b) > 0 {
		m, e := p.hdfsReader.Read(b)

		if m > 0 {
			n += m
			b = b[m:]
		}

		if e != nil {
			err = e
			break
		}
	}

	if err != nil && err == io.EOF && n > 0 {
		// no need to throw io.EOF
		Logdebug("RemoteFileProxy Finished reading", nil)
		err = nil
	}

	Logdebug("RemoteFileProxy ReadAt", p.file.logInfo(Fields{Operation: Read, MaxBytesToRead: maxBytesToRead,
		BytesRead: n, Error: err, Offset: off}))
	return n, err
}

func (p *RemoteROFileProxy) SeekToStart() (err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()

	err = p.hdfsReader.Seek(0)
	if err != nil {
		Logdebug("RemoteFileProxy SeekToStart failed", p.file.logInfo(Fields{Operation: SeekToStart, Offset: 0, Error: err}))
		return err
	} else {
		Logdebug("RemoteFileProxy SeekToStart", p.file.logInfo(Fields{Operation: SeekToStart, Offset: 0}))
		return nil
	}

}

func (p *RemoteROFileProxy) Read(b []byte) (n int, err error) {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()
	n, err = p.hdfsReader.Read(b)

	if err != nil {
		Logdebug("RemoteFileProxy Read", p.file.logInfo(Fields{Operation: Read, MaxBytesToRead: len(b), Error: err}))
		return n, err
	} else {
		Logdebug("RemoteFileProxy Read", p.file.logInfo(Fields{Operation: Read, MaxBytesToRead: len(b), TotalBytesRead: n}))
		return n, nil
	}
}

func (p *RemoteROFileProxy) Close() error {
	//NOTE: Locking is done in File.go
	err := p.hdfsReader.Close()
	if err != nil {
		Logdebug("RemoteFileProxy Close failed", p.file.logInfo(Fields{Operation: Close, Error: err}))
		return err
	} else {
		Logdebug("RemoteFileProxy Close", p.file.logInfo(Fields{Operation: Close}))
		return nil
	}
}

func (p *RemoteROFileProxy) Sync() error {
	p.file.lockFileHandles()
	defer p.file.unlockFileHandles()

	Logfatal("Sync API is not supported. Read only mode", nil)
	return nil
}
