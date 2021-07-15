// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s NAMENODE:PORT MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

var stagingDir string
var logLevel string
var rootCABundle string
var clientCertificate string
var clientKey string

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	retryPolicy := NewDefaultRetryPolicy(WallClock{})

	lazyMount := flag.Bool("lazy", false, "Allows to mount HDFS filesystem before HDFS is available")
	flag.DurationVar(&retryPolicy.TimeLimit, "retryTimeLimit", 5*time.Minute, "time limit for all retry attempts for failed operations")
	flag.IntVar(&retryPolicy.MaxAttempts, "retryMaxAttempts", 99999999, "Maxumum retry attempts for failed operations")
	flag.DurationVar(&retryPolicy.MinDelay, "retryMinDelay", 1*time.Second, "minimum delay between retries (note, first retry always happens immediatelly)")
	flag.DurationVar(&retryPolicy.MaxDelay, "retryMaxDelay", 60*time.Second, "maximum delay between retries")
	allowedPrefixesString := flag.String("allowedPrefixes", "*", "Comma-separated list of allowed path prefixes on the remote file system, "+
		"if specified the mount point will expose access to those prefixes only")
	expandZips := flag.Bool("expandZips", false, "Enables automatic expansion of ZIP archives")
	readOnly := flag.Bool("readOnly", false, "Enables mount with readonly")
	flag.StringVar(&logLevel, "logLevel", "error", "logs to be printed. error, warn, info, debug, trace")
	flag.StringVar(&stagingDir, "stageDir", "/tmp", "stage directory for writing files")
	tls := flag.Bool("tls", false, "Enables tls connections")
	flag.StringVar(&rootCABundle, "rootCABundle", "/srv/hops/super_crypto/hdfs/hops_root_ca.pem", "Root CA bundle location ")
	flag.StringVar(&clientCertificate, "clientCertificate", "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem", "Client certificate location")
	flag.StringVar(&clientKey, "clientKey", "/srv/hops/super_crypto/hdfs/hdfs_priv.pem", "Client key location")

	flag.Usage = Usage
	flag.Parse()

	log.Printf("Staging dir is:%s, Using TLS: %v  \n", stagingDir, *tls)

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}

	createStagingDir()

	log.Print("hdfs-mount: current head GITCommit: ", GITCOMMIT, ", Built time: ", BUILDTIME, ", Built by:", HOSTNAME)

	allowedPrefixes := strings.Split(*allowedPrefixesString, ",")

	retryPolicy.MaxAttempts += 1 // converting # of retry attempts to total # of attempts

	initLogger(logLevel, os.Stdout, false)

	tlsConfig := TLSConfig{
		TLS:               *tls,
		RootCABundle:      rootCABundle,
		ClientCertificate: clientCertificate,
		ClientKey:         clientKey,
	}

	hdfsAccessor, err := NewHdfsAccessor(flag.Arg(0), WallClock{}, tlsConfig)
	if err != nil {
		log.Fatal("Error/NewHdfsAccessor: ", err)
	}

	// Wrapping with FaultTolerantHdfsAccessor
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	if !*lazyMount && ftHdfsAccessor.EnsureConnected() != nil {
		log.Fatal("Can't establish connection to HDFS, mounting will NOT be performend (this can be suppressed with -lazy)")
	}

	// Creating the virtual file system
	fileSystem, err := NewFileSystem(ftHdfsAccessor, flag.Arg(1), allowedPrefixes, *expandZips, *readOnly, retryPolicy, WallClock{})
	if err != nil {
		log.Fatal("Error/NewFileSystem: ", err)
	}

	c, err := fileSystem.Mount()
	if err != nil {
		log.Fatal(err)
	}
	log.Print("Mounted successfully")

	// Increase the maximum number of file descriptor from 1K to 1M in Linux
	rLimit := syscall.Rlimit{
		Cur: 1024 * 1024,
		Max: 1024 * 1024}
	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logerror(fmt.Sprintf("Failed to update the maximum number of file descriptors from 1K to 1M, %v", err), Fields{})
	}

	defer func() {
		fileSystem.Unmount()
		log.Print("Closing...")
		c.Close()
		log.Print("Closed...")
	}()

	go func() {
		for x := range sigs {
			//Handling INT/TERM signals - trying to gracefully unmount and exit
			//TODO: before doing that we need to finish deferred flushes
			log.Print("Signal received: " + x.String())
			fileSystem.Unmount() // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

func createStagingDir() {
	if err := os.MkdirAll(stagingDir, 0700); err != nil {
		logerror(fmt.Sprintf("Failed to create stageDir: %s. Error: %v", stagingDir, err), Fields{})
	}
}
