// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
