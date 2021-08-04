package main

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"bazil.org/fuse/fs/fstestutil"
)

func TestSimple(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		testFile := filepath.Join(mountPoint, "somefile")
		loginfo(fmt.Sprintf("New file: %s", testFile), nil)
		createFile(t, testFile)
		fi, _ := os.Stat(testFile)
		fstat := fi.Sys().(*syscall.Stat_t)
		grupInfo, _ := user.LookupGroupId(fmt.Sprintf("%d", fstat.Gid))
		userInfo, _ := user.LookupId(fmt.Sprintf("%d", fstat.Uid))
		loginfo(fmt.Sprintf("New file: %s, User %s, Gropu %s", testFile, userInfo.Name, grupInfo.Name), nil)
		os.Remove(testFile)
	})
}

func TestMountSubDir(t *testing.T) {
	//mount and create some files and dirs
	dirs := 5
	filesPdir := 3
	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create some directories and files
		for i := 0; i < dirs; i++ {
			dir := filepath.Join(mountPoint, "dir"+strconv.Itoa(i))
			mkdir(t, dir)
			for j := 0; j < filesPdir; j++ {
				f := filepath.Join(dir, "file"+strconv.Itoa(j))
				createFile(t, f)
			}
		}

		content := listDir(t, mountPoint)
		if len(content) < dirs {
			t.Errorf("Failed. Expected >= %d, Got %d", dirs, len(content))
		}
	})

	// remount only one dir
	withMount(t, "/dir1", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		content := listDir(t, mountPoint)
		if len(content) != filesPdir {
			t.Errorf("Failed. Expected == %d, Got %d ", filesPdir, len(content))
			for _, ent := range content {
				loginfo(fmt.Sprintf("%s", ent.Name()), nil)

			}
		}
	})

	// remount every thing for cleanup
	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//delete all the files and dirs created in this test
		for i := 0; i < dirs; i++ {
			dir := filepath.Join(mountPoint, "dir"+strconv.Itoa(i))
			for j := 0; j < filesPdir; j++ {
				f := filepath.Join(dir, "file"+strconv.Itoa(j))
				rm(t, f)
			}
			rm(t, dir)
		}
	})
}

func withMount(t testing.TB, srcDir string, fn func(mntPath string, hdfsAccessor HdfsAccessor)) {
	initLogger("error", false, "")

	hdfsAccessor, err := NewHdfsAccessor("localhost:8020", WallClock{}, TLSConfig{TLS: false})
	if err != nil {
		logfatal(fmt.Sprintf("Error/NewHdfsAccessor: %v ", err), nil)
	}

	// Wrapping with FaultTolerantHdfsAccessor
	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	// Creating the virtual file system
	fileSystem, err := NewFileSystem(ftHdfsAccessor, srcDir, []string{"*"}, false, false, retryPolicy, WallClock{})
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
	fn(mnt.Dir, hdfsAccessor)
}

func mkdir(t testing.TB, dir string) {
	t.Helper()
	err := os.Mkdir(dir, 0700)
	if err != nil {
		t.Errorf("Faile to create directory %s. Error: %v", dir, err)
	}

}

func createFile(t testing.TB, filePath string) {
	t.Helper()
	out, err := os.Create(filePath)
	if err != nil {
		t.Errorf("Faile to create test file %s. Error: %v", filePath, err)
	}
	out.WriteString("This is some test string")
	out.Close()
}

func listDir(t testing.TB, dir string) []fs.FileInfo {
	t.Helper()
	content, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Errorf("Faile to list dir %s. Error: %v", dir, err)
	}
	return content
}

func rm(t testing.TB, path string) {
	t.Helper()
	err := os.Remove(path)
	if err != nil {
		t.Errorf("Faile to remove  %s. Error: %v", path, err)
	}
}
