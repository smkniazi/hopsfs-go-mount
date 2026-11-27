// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
	"syscall"
	"testing"
	"time"

	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// A file the mount knows that the backend no longer has is the one ENOENT on
// a known file. The sidecar logs at warning level by default, so anything
// quieter would leave no trace of it.
func TestAttrWarnsWhenAKnownFileIsGoneFromTheBackend(t *testing.T) {
	hooks := logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))
	t.Cleanup(func() { logrus.StandardLogger().ReplaceHooks(hooks) })
	// The suite's integration tests leave the logger at ERROR, below which a
	// warning never reaches a hook.
	level := logrus.GetLevel()
	logrus.SetLevel(logrus.WarnLevel)
	t.Cleanup(func() { logrus.SetLevel(level) })
	logs := logtest.NewGlobal()

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	hdfsAccessor.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{hdfsAccessor}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "gone", Attrs{Name: "gone", Mode: os.FileMode(0644), Expires: mockClock.Now()}).(*FileINode)
	mockClock.NotifyTimeElapsed(time.Second)

	hdfsAccessor.EXPECT().Stat("/gone").Return(Attrs{}, syscall.ENOENT)
	var attr fuse.Attr
	assert.Equal(t, syscall.ENOENT, file.Attr(nil, &attr))

	var warnings []string
	for _, entry := range logs.AllEntries() {
		if entry.Level == logrus.WarnLevel {
			warnings = append(warnings, entry.Message)
		}
	}
	assert.Equal(t, []string{"Known file not found on backend"}, warnings)
}

// reserveLikeTheAccessor makes a mock connector grant a reservation only
// while none is outstanding, as HdfsAccessorImpl does, and returns what a
// mocked call runs to take the reservation up.
func reserveLikeTheAccessor(connector *MockHdfsAccessor) (takeUp func()) {
	reserved := false
	connector.EXPECT().TryReserve().DoAndReturn(func() bool {
		if reserved {
			return false
		}
		reserved = true
		return true
	}).AnyTimes()
	return func() { reserved = false }
}

// failOnExtraConnector gives the file system room for one more connector and
// fails the test if it is added: with the only connector reserved and not
// yet called, a second request would add it.
func failOnExtraConnector(t *testing.T, mockCtrl *gomock.Controller) FileSystemOption {
	extra := NewMockHdfsAccessor(mockCtrl)
	extra.EXPECT().TryReserve().Return(true).AnyTimes()
	extra.EXPECT().OpenRead(gomock.Any()).Return(&MockReadSeekCloserWithPseudoRandomContent{FileSize: 5, ReaderStats: &ReaderStats{}}, nil).AnyTimes()
	extra.EXPECT().StatFs().Return(FsInfo{}, nil).AnyTimes()
	return WithExtraConnectors(1, func() (HdfsAccessor, error) {
		t.Error("a connector was added while the only one was reserved by a request that had not called it")
		return extra, nil
	})
}

// An open that downloads the file into the staging cache takes a connector
// for its one call only: one taken before the cache lookup and left unused
// while the download takes another would hold the open until the
// reservation lapses, or add a connector.
func TestOpenThroughTheStagingCacheTakesOneConnector(t *testing.T) {
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = newTestCache(t, 10)

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	connector := NewMockHdfsAccessor(mockCtrl)
	takeUp := reserveLikeTheAccessor(connector)
	connector.EXPECT().OpenRead("/cached").DoAndReturn(func(string) (ReadSeekCloser, error) {
		takeUp()
		return &MockReadSeekCloserWithPseudoRandomContent{FileSize: 5, ReaderStats: &ReaderStats{}}, nil
	})
	fs, _ := NewFileSystem([]HdfsAccessor{connector}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock,
		failOnExtraConnector(t, mockCtrl))
	root, _ := fs.Root()
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "cached", Attrs{Name: "cached", Mode: os.FileMode(0644), Size: 5}).(*FileINode)

	h, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.Nil(t, err)
	_, staged := h.(*FileHandle).File.fileProxy.(*LocalRWFileProxy)
	assert.True(t, staged, "the open did not download the file into the staging cache")
}

// stageInCache puts content for path in StagingCache under mtime, as the
// release of a read handle does.
func stageInCache(t *testing.T, path, content string, mtime time.Time) {
	t.Helper()
	staged, err := os.CreateTemp(t.TempDir(), "staged_*")
	assert.Nil(t, err)
	_, err = staged.WriteString(content)
	assert.Nil(t, err)
	StagingCache.Put(path, staged, int64(len(content)), mtime)
}

// newCachedFileTest gives a file system whose one file, /cached, is in the
// staging cache with the size and mtime the backend reports for it.
func newCachedFileTest(t *testing.T) (*MockHdfsAccessor, *MockClock, *DirINode, *FileINode) {
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = newTestCache(t, 10)

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{now: time.Now()}
	connector := NewMockHdfsAccessor(mockCtrl)
	connector.EXPECT().TryReserve().Return(true).AnyTimes()
	fs, _ := NewFileSystem([]HdfsAccessor{connector}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	mtime := time.Unix(1000, 0)
	file := root.(*DirINode).addOrUpdateChildInodeAttrs("unit_test", "cached",
		Attrs{Name: "cached", Mode: 0644, Size: 5, Mtime: mtime, Expires: mockClock.Now().Add(CacheAttrsTimeDuration)}).(*FileINode)
	connector.EXPECT().Stat("/cached").Return(Attrs{Name: "cached", Mode: 0644, Size: 5, Mtime: mtime}, nil).AnyTimes()
	stageInCache(t, "/cached", "hello", mtime)
	return connector, mockClock, root.(*DirINode), file
}

func openAndRelease(t *testing.T, file *FileINode, flags fuse.OpenFlags) error {
	t.Helper()
	h, err := file.Open(nil, &fuse.OpenRequest{Flags: flags}, &fuse.OpenResponse{})
	if err != nil {
		return err
	}
	_, staged := h.(*FileHandle).File.fileProxy.(*LocalRWFileProxy)
	assert.True(t, staged, "the open was not served from the staging cache")
	return h.(*FileHandle).Release(nil, &fuse.ReleaseRequest{})
}

// A read open served from the staging cache asks the namenode whether the
// connection user may read the file, at most once per attr TTL for a path, and
// fails with the denial while the entry stays cached. A write-only open asks
// nothing; the strict mock fails any CheckAccess it does not expect.
func TestStagingCacheHitChecksReadAccessOncePerTTL(t *testing.T) {
	connector, mockClock, _, file := newCachedFileTest(t)

	connector.EXPECT().CheckAccess("/cached", os.FileMode(0o4)).Return(nil)
	assert.Nil(t, openAndRelease(t, file, fuse.OpenReadOnly))
	assert.Nil(t, openAndRelease(t, file, fuse.OpenReadOnly))
	assert.Nil(t, openAndRelease(t, file, fuse.OpenWriteOnly))

	mockClock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	connector.EXPECT().CheckAccess("/cached", os.FileMode(0o4)).Return(syscall.EPERM)
	_, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadWrite}, &fuse.OpenResponse{})
	assert.Equal(t, syscall.EPERM, err)
	assert.True(t, StagingCache.Contains("/cached"))
}

// A passed check covers the path it was made for: after a rename the next read
// open of the same inode is checked again at its new path.
func TestReadAccessCheckIsRedoneAfterARename(t *testing.T) {
	connector, _, root, file := newCachedFileTest(t)

	connector.EXPECT().CheckAccess("/cached", os.FileMode(0o4)).Return(nil)
	assert.Nil(t, openAndRelease(t, file, fuse.OpenReadOnly))

	connector.EXPECT().Rename2("/cached", "/moved", gomock.Any()).Return(nil)
	assert.Nil(t, root.Rename(nil, &fuse.RenameRequest{OldName: "cached", NewName: "moved"}, root))

	connector.EXPECT().Stat("/moved").Return(Attrs{Name: "moved", Mode: 0644, Size: 5, Mtime: time.Unix(1000, 0)}, nil)
	connector.EXPECT().CheckAccess("/moved", os.FileMode(0o4)).Return(nil)
	assert.Nil(t, openAndRelease(t, file, fuse.OpenReadOnly))
}

// A read open that shares the proxy of a handle already open is checked the
// same way, while the handle opened before the denial keeps reading.
func TestSharedProxyOpenChecksReadAccess(t *testing.T) {
	connector, mockClock, _, file := newCachedFileTest(t)

	connector.EXPECT().CheckAccess("/cached", os.FileMode(0o4)).Return(nil)
	first, err := file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.Nil(t, err)
	_, err = file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.Nil(t, err)

	mockClock.NotifyTimeElapsed(CacheAttrsTimeDuration + time.Second)
	connector.EXPECT().CheckAccess("/cached", os.FileMode(0o4)).Return(syscall.EPERM)
	_, err = file.Open(nil, &fuse.OpenRequest{Flags: fuse.OpenReadOnly}, &fuse.OpenResponse{})
	assert.Equal(t, syscall.EPERM, err)

	// The FUSE server hands Read a buffer of the request size.
	resp := &fuse.ReadResponse{Data: make([]byte, 5)}
	assert.Nil(t, first.(*FileHandle).Read(nil, &fuse.ReadRequest{Offset: 0, Size: 5}, resp))
	assert.Equal(t, "hello", string(resp.Data))
}

// A flush of a new file uploads through the writer its create kept, and
// takes a connector only for the stat after the upload: one taken before the
// upload would keep every other request off it, or add a connector, for as
// long as the upload runs.
func TestFlushOfANewFileHoldsNoConnectorDuringTheUpload(t *testing.T) {
	originalCache := StagingCache
	t.Cleanup(func() { StagingCache = originalCache })
	StagingCache = newTestCache(t, 10)

	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	connector := NewMockHdfsAccessor(mockCtrl)
	takeUp := reserveLikeTheAccessor(connector)
	fileName := "/flushedFile"
	var fs *FileSystem
	requestedDuringUpload := false
	writer := NewMockHdfsWriter(mockCtrl)
	writer.EXPECT().Write(gomock.Any()).DoAndReturn(func(p []byte) (int, error) {
		if !requestedDuringUpload {
			requestedDuringUpload = true
			_, err := fs.getDFSConnector().StatFs()
			assert.Nil(t, err)
		}
		return len(p), nil
	}).AnyTimes()
	writer.EXPECT().Close().Return(nil).AnyTimes()
	connector.EXPECT().CreateFileWithGroup(fileName, gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(string, os.FileMode, bool, string) (HdfsWriter, error) { takeUp(); return writer, nil }).AnyTimes()
	connector.EXPECT().Remove(fileName).DoAndReturn(func(string) error { takeUp(); return nil }).AnyTimes()
	connector.EXPECT().Stat(fileName).DoAndReturn(func(string) (Attrs, error) { takeUp(); return Attrs{Name: fileName}, nil }).AnyTimes()
	connector.EXPECT().Chown(fileName, gomock.Any(), gomock.Any()).DoAndReturn(func(string, string, string) error { takeUp(); return nil }).AnyTimes()
	connector.EXPECT().StatFs().DoAndReturn(func() (FsInfo, error) { takeUp(); return FsInfo{}, nil }).AnyTimes()
	fs, _ = NewFileSystem([]HdfsAccessor{connector}, "/", []string{"*"}, false, DelaySyncUntilClose, NewDefaultRetryPolicy(mockClock), mockClock,
		failOnExtraConnector(t, mockCtrl))

	root, _ := fs.Root()
	_, h, err := root.(*DirINode).Create(nil, &fuse.CreateRequest{Name: fileName,
		Flags: fuse.OpenReadWrite | fuse.OpenCreate, Mode: os.FileMode(0644)}, &fuse.CreateResponse{})
	assert.Nil(t, err)
	fileHandle := h.(*FileHandle)
	assert.Nil(t, fileHandle.Write(nil, &fuse.WriteRequest{Data: []byte("hello world"), Offset: 0}, &fuse.WriteResponse{}))
	assert.Nil(t, fileHandle.Flush(nil, nil))
	assert.True(t, requestedDuringUpload, "the flush did not upload through the writer")
	assert.Nil(t, fileHandle.Release(nil, nil))
}
