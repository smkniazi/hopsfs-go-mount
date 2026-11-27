// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
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
