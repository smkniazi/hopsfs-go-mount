package main

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"syscall"
	"testing"

	"bazil.org/fuse/fs/fstestutil"
)

func TestSimple(t *testing.T) {

	withMount(t, func(mountPoint string) {

		//create a file, make sure that use and group information is correct
		testFile := filepath.Join(mountPoint, "somefile")
		loginfo(fmt.Sprintf("New file: %s", testFile), nil)

		file, err := os.Create(testFile)
		if err != nil {
			t.Fatalf("Faile to create test file. Error: %v", err)
		}
		file.WriteString("This is some test string")
		file.Close()

		fi, _ := os.Stat(testFile)
		fstat := fi.Sys().(*syscall.Stat_t)
		grupInfo, _ := user.LookupGroupId(fmt.Sprintf("%d", fstat.Gid))
		userInfo, _ := user.LookupId(fmt.Sprintf("%d", fstat.Uid))
		loginfo(fmt.Sprintf("New file: %s, User %s, Gropu %s", testFile, userInfo.Name, grupInfo.Name), nil)
		os.Remove(testFile)
	})
}

func withMount(t testing.TB, fn func(mntPath string)) {
	initLogger("trace", os.Stdout, false)

	hdfsAccessor, err := NewHdfsAccessor("localhost:8020", WallClock{}, TLSConfig{TLS: false})
	if err != nil {
		logfatal(fmt.Sprintf("Error/NewHdfsAccessor: %v ", err), nil)
	}

	// Wrapping with FaultTolerantHdfsAccessor
	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	// Creating the virtual file system
	fileSystem, err := NewFileSystem(ftHdfsAccessor, []string{"*"}, false, false, retryPolicy, WallClock{})
	if err != nil {
		logfatal(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := getMountOptions(false)
	mnt, err := fstestutil.MountedT(t, fileSystem, nil, mountOptions...)
	if err != nil {
		t.Fatal(fmt.Sprintf("Unable to mount fs: Error %v", err), nil)

	}
	defer mnt.Close()
	loginfo(fmt.Sprintf("Connected to HopsFS. Mount point is %s", mnt.Dir), nil)
	fn(mnt.Dir)
}
