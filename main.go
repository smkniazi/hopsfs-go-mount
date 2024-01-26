// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

const (
	STAT_CACHE_TIME = 5 * time.Second
)

func main() {

	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	parseArgsAndInitLogger(retryPolicy)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	hopsRpcAddress := flag.Arg(0)
	mountPoint := flag.Arg(1)
	createStagingDir()

	allowedPrefixes := strings.Split(allowedPrefixesString, ",")

	tlsConfig := TLSConfig{
		TLS:               tls,
		RootCABundle:      rootCABundle,
		ClientCertificate: clientCertificate,
		ClientKey:         clientKey,
	}

	ftHdfsAccessors := make([]HdfsAccessor, connectors)

	for i := 0; i < connectors; i++ {
		hdfsAccessor, err := NewHdfsAccessor(hopsRpcAddress, WallClock{}, tlsConfig)
		if err != nil {
			logfatal(fmt.Sprintf("Error/NewHopsFSAccessor: %v ", err), nil)
		}
		ftHdfsAccessors[i] = NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)
	}
	loginfo(fmt.Sprintf("Create %d file system clients", len(ftHdfsAccessors)), nil)

	if strings.Compare(mntSrcDir, "/") != 0 {
		err := checkSrcMountPath(ftHdfsAccessors[0])
		if err != nil {
			logfatal(fmt.Sprintf("Unable to mount the file system as source mount directory is not accessible. Error: %v ", err), nil)
		}
	}

	// Wrapping with FaultTolerantHdfsAccessor

	if !lazyMount && ftHdfsAccessors[0].EnsureConnected() != nil {
		logfatal("Can't establish connection to HopsFS, mounting will NOT be performend (this can be suppressed with -lazy", nil)
	}

	// Creating the virtual file system
	fileSystem, err := NewFileSystem(ftHdfsAccessors, mntSrcDir, allowedPrefixes, readOnly, retryPolicy, WallClock{})
	if err != nil {
		logfatal(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := getMountOptions(readOnly)
	c, err := fileSystem.Mount(mountPoint, mountOptions...)
	if err != nil {
		logfatal(fmt.Sprintf("Failed to mount FS. Error: %v", err), nil)
	}
	loginfo(fmt.Sprintf("Mounted successfully. HopsFS src dir: %s ", mntSrcDir), nil)

	// Increase the maximum number of file descriptor from 1K to 1M in Linux
	rLimit := syscall.Rlimit{
		Cur: 1024 * 1024,
		Max: 1024 * 1024}
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logerror(fmt.Sprintf("Failed to update the maximum number of file descriptors from 1K to 1M, %v", err), Fields{})
	}

	defer func() {
		fileSystem.Unmount(mountPoint)
		loginfo("Closing...", nil)
		c.Close()
		loginfo("Closed...", nil)
	}()

	go func() {
		for x := range sigs {
			//Handling INT/TERM signals - trying to gracefully unmount and exit
			//TODO: before doing that we need to finish deferred flushes
			loginfo(fmt.Sprintf("Received signal: %s", x.String()), nil)
			fileSystem.Unmount(mountPoint) // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		logfatal(fmt.Sprintf("Failed to serve FS. Error: %v", err), nil)
	}
}

func getMountOptions(ro bool) []fuse.MountOption {
	mountOptions := []fuse.MountOption{fuse.FSName("hopsfs"),
		fuse.Subtype("hopsfs"),
		// fuse.WritebackCache(), // write to kernel cache, improves performance for small writes.
		// NOTE: It creates problem when reading file updated by external clients
		// https://www.kernel.org/doc/Documentation/filesystems/fuse-io.txt
		fuse.MaxReadahead(1024 * 64), //TODO: make configurable
		fuse.DefaultPermissions(),
	}

	if allowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}

	if ro {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}
	return mountOptions
}

func createStagingDir() {
	if err := os.MkdirAll(stagingDir, 0700); err != nil {
		logerror(fmt.Sprintf("Failed to create stageDir: %s. Error: %v", stagingDir, err), Fields{})
	}
}

func checkSrcMountPath(hdfsAccessor HdfsAccessor) error {
	_, err := hdfsAccessor.Stat(mntSrcDir)
	if err != nil {
		return err
	}
	return nil
}
