package main

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
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
		createFile(t, testFile, "some data")
		fi, _ := os.Stat(testFile)
		fstat := fi.Sys().(*syscall.Stat_t)
		grupInfo, _ := user.LookupGroupId(fmt.Sprintf("%d", fstat.Gid))
		userInfo, _ := user.LookupId(fmt.Sprintf("%d", fstat.Uid))
		loginfo(fmt.Sprintf("New file: %s, User %s, Gropu %s", testFile, userInfo.Name, grupInfo.Name), nil)
		os.Remove(testFile)
	})
}

// testing multiple read write clients perfile
func TestMultipleRWCllients(t *testing.T) {

	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {
		//create a file, make sure that use and group information is correct
		// mountPoint = "/tmp"
		testFile1 := filepath.Join(mountPoint, "somefile")
		testFile2 := filepath.Join(mountPoint, "somefile.bak")
		loginfo(fmt.Sprintf("New file: %s", testFile1), nil)
		createFile(t, testFile1, "initial data\nadsf\n")

		c1, _ := os.OpenFile(testFile1, os.O_RDWR, 0600)
		c2, _ := os.OpenFile(testFile1, os.O_RDWR, 0600)
		c3, _ := os.OpenFile(testFile1, os.O_RDWR, 0600)

		c1.WriteString("First client\n")
		c1.Close()

		os.Rename(testFile1, testFile2)

		c2.WriteString("Second client\nSecond client\n")
		c3.WriteString("Third client\nThird client\nThird Client\n")
		c2.Close()
		c3.Close()

		c5, err := os.Open(testFile2)

		if err != nil {
			t.Error("The file should have opened successfully")
		} else {
			loginfo("File opened successfully", nil)
			buffer := make([]byte, 1024)
			c5.Read(buffer)
			//fmt.Printf("%s", buffer)
		}
		c5.Close()

		os.Remove(testFile1)
		os.Remove(testFile2)
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
				createFile(t, f, "initial data")
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
				loginfo(fmt.Sprintf("file/dir %s", ent.Name()), nil)
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
				rmFile(t, f)
			}
			rmFile(t, dir)
		}
	})
}

func TestGitClone(t *testing.T) {
	withMount(t, "/", func(mountPoint string, hdfsAccessor HdfsAccessor) {

		cloneDir := "cloneDir"
		fullPath := filepath.Join(mountPoint, cloneDir)

		//delete the dir if it already exists
		_, err := os.Stat(fullPath)
		if os.IsExist(err) {
			err := rmDir(t, fullPath)
			if err != nil {
				t.Errorf("Faile to remove  %s. Error: %v", fullPath, err)
			}
		}

		_, err = exec.Command("git", "clone", "https://github.com/logicalclocks/ndb-chef", fullPath).Output()
		if err != nil {
			t.Errorf("Unable to clone the repo. Error: %v", err)
		}

		//clean
		err = rmDir(t, fullPath)
		if err != nil {
			t.Errorf("Faile to remove  %s. Error: %v", fullPath, err)
		}
	})
}

func withMount(t testing.TB, srcDir string, fn func(mntPath string, hdfsAccessor HdfsAccessor)) {
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

func createFile(t testing.TB, filePath string, data string) {
	t.Helper()
	out, err := os.Create(filePath)
	if err != nil {
		t.Errorf("Faile to create test file %s. Error: %v", filePath, err)
	}
	out.WriteString(data)
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

func rmFile(t testing.TB, path string) {
	t.Helper()
	err := os.Remove(path)
	if err != nil {
		t.Errorf("Faile to remove  %s. Error: %v", path, err)
	}
}

func rmDir(t testing.TB, dir string) error {
	t.Helper()
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}

	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}

	err = os.Remove(dir)
	if err != nil {
		return err
	}

	return nil
}
