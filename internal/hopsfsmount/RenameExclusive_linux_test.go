// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"golang.org/x/sys/unix"
)

// renameat2(RENAME_NOREPLACE) through the mount renames onto a missing path and fails with EEXIST
// onto an existing one, leaving both files as they were.
func TestRenameNoReplace(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		src := filepath.Join(mountPoint, uniqueTestName("noreplace_src"))
		other := filepath.Join(mountPoint, uniqueTestName("noreplace_other"))
		dst := filepath.Join(mountPoint, uniqueTestName("noreplace_dst"))
		defer os.Remove(src)
		defer os.Remove(other)
		defer os.Remove(dst)
		if err := createFile(src, "src"); err != nil {
			t.Fatalf("create %s: %v", src, err)
		}
		if err := createFile(other, "other"); err != nil {
			t.Fatalf("create %s: %v", other, err)
		}

		if err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE); err != nil {
			t.Fatalf("rename onto a missing path: %v", err)
		}
		err := unix.Renameat2(unix.AT_FDCWD, other, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE)
		if !errors.Is(err, unix.EEXIST) {
			t.Fatalf("rename onto an existing path. Expecting: EEXIST, Got: %v", err)
		}
		if got, _ := readFile(t, dst); got != "src" {
			t.Fatalf("the destination was replaced. Expecting: src, Got: %s", got)
		}
		if got, _ := readFile(t, other); got != "other" {
			t.Fatalf("the source of the failed rename changed. Expecting: other, Got: %s", got)
		}
	})
}

// Two mounts, as two pods have, rename their own file onto one path with RENAME_NOREPLACE at once:
// neither kernel knows the path, so both renames reach the namenode, and exactly one wins.
func TestRenameNoReplaceAcrossMounts(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountA string, _ HdfsAccessor) {
		withMount(t, "/", DelaySyncUntilClose, func(mountB string, _ HdfsAccessor) {
			for round := 0; round < 10; round++ {
				srcA := filepath.Join(mountA, uniqueTestName("across_src_a"))
				srcB := filepath.Join(mountB, uniqueTestName("across_src_b"))
				dstName := uniqueTestName("across_dst")
				if err := createFile(srcA, "a"); err != nil {
					t.Fatalf("create %s: %v", srcA, err)
				}
				if err := createFile(srcB, "b"); err != nil {
					t.Fatalf("create %s: %v", srcB, err)
				}

				errs := make([]error, 2)
				var start, done sync.WaitGroup
				start.Add(1)
				for i, mv := range [][2]string{{srcA, filepath.Join(mountA, dstName)}, {srcB, filepath.Join(mountB, dstName)}} {
					done.Add(1)
					go func(i int, from, to string) {
						defer done.Done()
						start.Wait()
						errs[i] = unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
					}(i, mv[0], mv[1])
				}
				start.Done()
				done.Wait()

				winners := 0
				for _, err := range errs {
					if err == nil {
						winners++
					} else if !errors.Is(err, unix.EEXIST) {
						t.Fatalf("round %d: a losing rename failed with %v. Expecting: EEXIST", round, err)
					}
				}
				if winners != 1 {
					t.Fatalf("round %d: %d renames onto %s won", round, winners, dstName)
				}
				os.Remove(srcA)
				os.Remove(srcB)
				os.Remove(filepath.Join(mountA, dstName))
			}
		})
	})
}

// Two mounts create one path with O_CREAT|O_EXCL at once: exactly one create wins, the other gets
// EEXIST.
func TestCreateExclusiveAcrossMounts(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountA string, _ HdfsAccessor) {
		withMount(t, "/", DelaySyncUntilClose, func(mountB string, _ HdfsAccessor) {
			for round := 0; round < 10; round++ {
				name := uniqueTestName("excl")
				files := make([]*os.File, 2)
				errs := make([]error, 2)
				var start, done sync.WaitGroup
				start.Add(1)
				for i, mountPoint := range []string{mountA, mountB} {
					done.Add(1)
					go func(i int, path string) {
						defer done.Done()
						start.Wait()
						files[i], errs[i] = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
					}(i, filepath.Join(mountPoint, name))
				}
				start.Done()
				done.Wait()

				winners := 0
				for i, err := range errs {
					if err == nil {
						winners++
						files[i].Close()
					} else if !errors.Is(err, unix.EEXIST) {
						t.Fatalf("round %d: a losing create failed with %v. Expecting: EEXIST", round, err)
					}
				}
				if winners != 1 {
					t.Fatalf("round %d: %d creates of %s won", round, winners, name)
				}
				os.Remove(filepath.Join(mountA, name))
			}
		})
	})
}

// A rename onto a non-empty directory fails with ENOTEMPTY, which the mount does not retry.
func TestRenameOntoNonEmptyDirectoryIsENOTEMPTY(t *testing.T) {
	withMount(t, "/", DelaySyncUntilClose, func(mountPoint string, hdfsAccessor HdfsAccessor) {
		src := filepath.Join(mountPoint, uniqueTestName("notempty_src"))
		dst := filepath.Join(mountPoint, uniqueTestName("notempty_dst"))
		defer os.RemoveAll(src)
		defer os.RemoveAll(dst)
		mkdir(t, src)
		mkdir(t, dst)
		if err := createFile(filepath.Join(dst, "file"), "data"); err != nil {
			t.Fatalf("create in %s: %v", dst, err)
		}

		// os.Rename refuses an existing directory destination itself, without the syscall.
		err := unix.Rename(src, dst)
		if !errors.Is(err, unix.ENOTEMPTY) {
			t.Fatalf("rename onto a non-empty directory. Expecting: ENOTEMPTY, Got: %v", err)
		}
	})
}
