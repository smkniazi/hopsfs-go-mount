// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setForTest sets *v to value until the test ends.
func setForTest[T any](t testing.TB, v *T, value T) {
	t.Helper()
	previous := *v
	*v = value
	t.Cleanup(func() { *v = previous })
}

func writeBackendFile(t testing.TB, accessor HdfsAccessor, path, content string, mode os.FileMode) {
	t.Helper()
	w, err := accessor.CreateFile(path, mode, true)
	require.NoError(t, err)
	_, err = w.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	// The create applies the client's umask.
	require.NoError(t, accessor.Chmod(path, mode))
}

// catAs runs cat on path as uid and gid, which need no passwd entry: the
// kernel's default_permissions check compares the numbers.
func catAs(uid uint32, path string) (string, error) {
	cmd := exec.Command("cat", path)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid}}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// A mount restricted to one prefix refuses mkdir, create and rename to a name
// outside it with EPERM, and the backend keeps no trace of the attempts, while
// the same operations inside the prefix go through.
func TestMountRefusesNamespaceChangesOutsideAllowedPrefixes(t *testing.T) {
	backend := connectBackend(t)
	base := uniqueBackendBase("prefixes")
	ensureBackendDir(t, backend, base+"/allowed", base)
	writeBackendFile(t, backend, base+"/allowed/file", "content", 0644)

	prefix := strings.TrimPrefix(base, "/") + "/allowed"
	withMountAndPrefixes(t, base, DelaySyncUntilClose, []string{prefix}, func(mnt string, _ HdfsAccessor) {
		assert.ErrorIs(t, os.Mkdir(filepath.Join(mnt, "hidden"), 0755), syscall.EPERM)
		_, err := os.Create(filepath.Join(mnt, "hidden"))
		assert.ErrorIs(t, err, syscall.EPERM)
		assert.ErrorIs(t, os.Rename(filepath.Join(mnt, "allowed", "file"), filepath.Join(mnt, "hidden")), syscall.EPERM)
		assert.ErrorIs(t, os.Rename(filepath.Join(mnt, "allowed"), filepath.Join(mnt, "hidden")), syscall.EPERM)

		_, err = backend.Stat(base + "/hidden")
		assert.ErrorIs(t, err, syscall.ENOENT)
		_, err = backend.Stat(base + "/allowed/file")
		assert.NoError(t, err)

		assert.NoError(t, os.Mkdir(filepath.Join(mnt, "allowed", "child"), 0755))
		assert.NoError(t, os.Rename(filepath.Join(mnt, "allowed", "file"), filepath.Join(mnt, "allowed", "child", "file")))
		_, err = backend.Stat(base + "/allowed/child/file")
		assert.NoError(t, err)
	})
}

// The mount point shows the mode of the mounted directory, and follows a
// change of it once the attrs expire.
func TestMountRootShowsTheMountedDirectory(t *testing.T) {
	setForTest(t, &CacheAttrsTimeDuration, time.Second)
	backend := connectBackend(t)
	base := uniqueBackendBase("root")
	ensureBackendDir(t, backend, base, base)
	require.NoError(t, backend.Chmod(base, 0751))

	withMount(t, base, DelaySyncUntilClose, func(mnt string, _ HdfsAccessor) {
		fi, err := os.Stat(mnt)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0751), fi.Mode().Perm())

		require.NoError(t, backend.Chmod(base, 0750))
		time.Sleep(2 * CacheAttrsTimeDuration)
		fi, err = os.Stat(mnt)
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0750), fi.Mode().Perm())
	})
}

// With the mount connected as an ordinary HopsFS user, a file read through it
// is kept in the staging cache. Once HopsFS stops letting that user read it,
// by a chmod or a chown, a new open fails with EPERM after the attrs expire,
// whether it would share an open handle's proxy or take the cached copy, while
// the handle opened before keeps reading.
func TestMountChecksReadAccessOfCachedFiles(t *testing.T) {
	setForTest(t, &CacheAttrsTimeDuration, time.Second)
	superuser := connectBackend(t)
	base := uniqueBackendBase("cachedread")
	ensureBackendDir(t, superuser, base, base)
	require.NoError(t, superuser.Chmod(base, 0777))
	revocations := map[string]func(path string) error{
		"chmod": func(path string) error { return superuser.Chmod(path, 0200) },
		"chown": func(path string) error { return superuser.Chown(path, "gohdfs2", "") },
	}
	for name := range revocations {
		writeBackendFile(t, superuser, base+"/"+name, "content", 0600)
		require.NoError(t, superuser.Chown(base+"/"+name, "gohdfs1", ""))
	}

	// Runs after ForceOverrideUsername is restored, so later tests connect as
	// the suite's user again.
	t.Cleanup(func() { _ = InitConnectionUser() })
	setForTest(t, &ForceOverrideUsername, "gohdfs1")
	withMount(t, base, DelaySyncUntilClose, func(mnt string, _ HdfsAccessor) {
		for name, revoke := range revocations {
			path := filepath.Join(mnt, name)
			data, err := os.ReadFile(path)
			require.NoError(t, err, name)
			require.Equal(t, "content", string(data))
			open, err := os.Open(path)
			require.NoError(t, err, name)

			require.NoError(t, revoke(base+"/"+name))
			time.Sleep(2 * CacheAttrsTimeDuration)

			_, err = os.ReadFile(path)
			assert.ErrorIs(t, err, syscall.EPERM, "%s: open sharing a handle's proxy", name)
			buf := make([]byte, len("content"))
			n, err := open.ReadAt(buf, 0)
			assert.NoError(t, err, name)
			assert.Equal(t, "content", string(buf[:n]), "%s: handle opened before", name)
			require.NoError(t, open.Close())

			_, err = os.ReadFile(path)
			assert.ErrorIs(t, err, syscall.EPERM, "%s: open of the cached copy", name)
		}
	})
}

// With allow_other and default_permissions another local user reads through
// the mount what the file's mode lets "other" read, and the kernel refuses it
// once a chmod in HopsFS shows after the attrs expire. Without
// default_permissions every local user acts as the mount's HopsFS user.
func TestMountSharedUserFollowsTheBackendMode(t *testing.T) {
	setForTest(t, &CacheAttrsTimeDuration, time.Second)
	setForTest(t, &AllowOther, true)
	backend := connectBackend(t)
	base := uniqueBackendBase("shareduser")
	ensureBackendDir(t, backend, base, base)
	require.NoError(t, backend.Chmod(base, 0755))

	for _, defaultPermissions := range []bool{true, false} {
		setForTest(t, &EnableDefaultPermissions, defaultPermissions)
		file := base + "/file"
		writeBackendFile(t, backend, file, "content", 0644)
		withMount(t, base, DelaySyncUntilClose, func(mnt string, _ HdfsAccessor) {
			path := filepath.Join(mnt, "file")
			out, err := catAs(4242, path)
			require.NoError(t, err, out)
			assert.Equal(t, "content", out)

			require.NoError(t, backend.Chmod(file, 0600))
			time.Sleep(2 * CacheAttrsTimeDuration)
			out, err = catAs(4242, path)
			if defaultPermissions {
				assert.Error(t, err, "uid 4242 read a 0600 file: %s", out)
				assert.Contains(t, out, "Permission denied")
			} else {
				assert.NoError(t, err, out)
				assert.Equal(t, "content", out)
			}
		})
	}
}
