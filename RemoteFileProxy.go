package main

type RemoteFileProxy struct {
}

var _ FileProxy = (*RemoteFileProxy)(nil)

func (p *RemoteFileProxy) Truncate(size int64) error {
	logfatal("Not implemented yet", nil)
	return nil
}
func (p *RemoteFileProxy) WriteAt(b []byte, off int64) (n int, err error) {
	logfatal("Not implemented yet", nil)
	return 0, nil
}
func (p *RemoteFileProxy) ReadAt(b []byte, off int64) (n int, err error) {
	logfatal("Not implemented yet", nil)
	return 0, nil
}
func (p *RemoteFileProxy) Seek(offset int64, whence int) (ret int64, err error) {
	logfatal("Not implemented yet", nil)
	return 0, nil
}
func (p *RemoteFileProxy) Read(b []byte) (n int, err error) {
	logfatal("Not implemented yet", nil)
	return 0, nil
}
func (p *RemoteFileProxy) Close() error {
	logfatal("Not implemented yet", nil)
	return nil
}
func (p *RemoteFileProxy) Sync() error {
	logfatal("Not implemented yet", nil)
	return nil
}
