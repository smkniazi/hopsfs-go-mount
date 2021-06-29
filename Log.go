// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"io"

	logger "github.com/sirupsen/logrus"
)

// bunch of constant for logging
const (
	Path           = "path"
	Operation      = "op"
	Mode           = "mode"
	Flags          = "flags"
	Bytes          = "bytes"
	ReadDir        = "read_dir"
	Read           = "read"
	ReadArch       = "read_archive"
	OpenArch       = "open_archive"
	ReadHandle     = "create_read_handle"
	Write          = "write"
	WriteHandle    = "create_write_handle"
	Open           = "open"
	Remove         = "remove"
	Create         = "create"
	Rename         = "rename"
	Chmod          = "chmod"
	Chown          = "chown"
	Fsync          = "fsync"
	Flush          = "flush"
	Close          = "close"
	Stat           = "stat"
	StatFS         = "statfs"
	UID            = "uid"
	GID            = "gid"
	User           = "user"
	Holes          = "holes"
	Seeks          = "seeks"
	HardSeeks      = "hard_seeks"
	CacheHits      = "cache_hits"
	TmpFile        = "tmp_file"
	Archive        = "zip_file"
	Error          = "error"
	RetryingPolicy = "retry_policy"
	Message        = "msg"
	Retries        = "retries"
	Diag           = "diag"
	Delay          = "delay"
)

func initLogger(l string, out io.Writer) {
	lvl, err := logger.ParseLevel(l)
	if err != nil {
		logger.Errorf("Invlid log level %s ", l)
		lvl = logger.WarnLevel
	}

	// Log as JSON instead of the default ASCII formatter.
	// Logger.SetFormatter(&logrus.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	// TODO log to file and log cutting
	logger.SetOutput(out)

	// Only log the warning severity or above.
	logger.SetLevel(lvl)
}
