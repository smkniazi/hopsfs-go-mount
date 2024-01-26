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

	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount"
)

func main() {
	retryPolicy := hopsfsmount.NewDefaultRetryPolicy(hopsfsmount.WallClock{})
	hopsfsmount.ParseArgsAndInitLogger(retryPolicy)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	hopsRpcAddress := flag.Arg(0)
	mountPoint := flag.Arg(1)
	createStagingDir()

	allowedPrefixes := strings.Split(hopsfsmount.AllowedPrefixesString, ",")

	tlsConfig := hopsfsmount.TLSConfig{
		TLS:               hopsfsmount.Tls,
		RootCABundle:      hopsfsmount.RootCABundle,
		ClientCertificate: hopsfsmount.ClientCertificate,
		ClientKey:         hopsfsmount.ClientKey,
	}

	ftHdfsAccessors := make([]hopsfsmount.HdfsAccessor, hopsfsmount.Connectors)

	for i := 0; i < hopsfsmount.Connectors; i++ {
		hdfsAccessor, err := hopsfsmount.NewHdfsAccessor(hopsRpcAddress, hopsfsmount.WallClock{}, tlsConfig)
		if err != nil {
			hopsfsmount.Logfatal(fmt.Sprintf("Error/NewHopsFSAccessor: %v ", err), nil)
		}
		ftHdfsAccessors[i] = hopsfsmount.NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)
	}
	hopsfsmount.Loginfo(fmt.Sprintf("Create %d file system clients", len(ftHdfsAccessors)), nil)

	if strings.Compare(hopsfsmount.MntSrcDir, "/") != 0 {
		err := checkSrcMountPath(ftHdfsAccessors[0])
		if err != nil {
			hopsfsmount.Logfatal(fmt.Sprintf("Unable to mount the file system as source mount directory is not accessible. Error: %v ", err), nil)
		}
	}

	// Wrapping with FaultTolerantHdfsAccessor

	if !hopsfsmount.LazyMount && ftHdfsAccessors[0].EnsureConnected() != nil {
		hopsfsmount.Logfatal("Can't establish connection to HopsFS, mounting will NOT be performend (this can be suppressed with -lazy", nil)
	}

	// Creating the virtual file system
	fileSystem, err := hopsfsmount.NewFileSystem(ftHdfsAccessors, hopsfsmount.MntSrcDir, allowedPrefixes, hopsfsmount.ReadOnly, retryPolicy, hopsfsmount.WallClock{})
	if err != nil {
		hopsfsmount.Logfatal(fmt.Sprintf("Error/NewFileSystem: %v ", err), nil)
	}

	mountOptions := hopsfsmount.GetMountOptions(hopsfsmount.ReadOnly)
	c, err := fileSystem.Mount(mountPoint, mountOptions...)
	if err != nil {
		hopsfsmount.Logfatal(fmt.Sprintf("Failed to mount FS. Error: %v", err), nil)
	}
	hopsfsmount.Loginfo(fmt.Sprintf("Mounted successfully. HopsFS src dir: %s ", hopsfsmount.MntSrcDir), nil)

	// Increase the maximum number of file descriptor from 1K to 1M in Linux
	rLimit := syscall.Rlimit{
		Cur: 1024 * 1024,
		Max: 1024 * 1024}
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		hopsfsmount.Logerror(fmt.Sprintf("Failed to update the maximum number of file descriptors from 1K to 1M, %v", err), hopsfsmount.Fields{})
	}

	defer func() {
		fileSystem.Unmount(mountPoint)
		hopsfsmount.Loginfo("Closing...", nil)
		c.Close()
		hopsfsmount.Loginfo("Closed...", nil)
	}()

	go func() {
		for x := range sigs {
			//Handling INT/TERM signals - trying to gracefully unmount and exit
			//TODO: before doing that we need to finish deferred flushes
			hopsfsmount.Loginfo(fmt.Sprintf("Received signal: %s", x.String()), nil)
			fileSystem.Unmount(mountPoint) // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		hopsfsmount.Logfatal(fmt.Sprintf("Failed to serve FS. Error: %v", err), nil)
	}
}

func createStagingDir() {
	if err := os.MkdirAll(hopsfsmount.StagingDir, 0700); err != nil {
		hopsfsmount.Logerror(fmt.Sprintf("Failed to create stageDir: %s. Error: %v", hopsfsmount.StagingDir, err), hopsfsmount.Fields{})
	}
}

func checkSrcMountPath(hdfsAccessor hopsfsmount.HdfsAccessor) error {
	_, err := hdfsAccessor.Stat(hopsfsmount.MntSrcDir)
	if err != nil {
		return err
	}
	return nil
}
