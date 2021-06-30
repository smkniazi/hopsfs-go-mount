// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"io"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logger "github.com/sirupsen/logrus"
)

// bunch of constant for logging
const (
	Path              = "path"
	Operation         = "op"
	Mode              = "mode"
	Flags             = "flags"
	Bytes             = "bytes"
	ReadDir           = "read_dir"
	Read              = "read"
	ReadArch          = "read_archive"
	OpenArch          = "open_archive"
	ReadHandle        = "create_read_handle"
	Write             = "write"
	WriteHandle       = "create_write_handle"
	Open              = "open"
	Remove            = "remove"
	Create            = "create"
	Rename            = "rename"
	Chmod             = "chmod"
	Chown             = "chown"
	Fsync             = "fsync"
	Flush             = "flush"
	Close             = "close"
	Stat              = "stat"
	StatFS            = "statfs"
	UID               = "uid"
	GID               = "gid"
	User              = "user"
	Holes             = "holes"
	Seeks             = "seeks"
	HardSeeks         = "hard_seeks"
	CacheHits         = "cache_hits"
	TmpFile           = "tmp_file"
	Archive           = "zip_file"
	Error             = "error"
	RetryingPolicy    = "retry_policy"
	Message           = "msg"
	Retries           = "retries"
	Diag              = "diag"
	Delay             = "delay"
	Entries           = "entries"
	Truncate          = "truncate"
	TotalBytesRead    = "total_bytes_read"
	TotalBytesWritten = "total_bytes_written"
	FileSize          = "file_size"
)

func initLogger(l string, out io.Writer) {
	lvl, err := logger.ParseLevel(l)
	if err != nil {
		logger.Errorf("Invlid log level %s ", l)
		lvl = logger.WarnLevel
	}

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	// TODO log to file and log cutting
	logger.SetOutput(out)

	//Json
	// logger.SetFormatter(&logger.JSONFormatter{})

	//set custom formatter github.com/antonfisher/nested-logrus-formatter
	logger.SetFormatter(&nested.Formatter{
		HideKeys:       false,
		NoFieldsColors: true,
		FieldsOrder:    []string{"msg", Operation, Path, Bytes, TotalBytesRead, TotalBytesWritten},
	})

	// Only log the warning severity or above.
	logger.SetLevel(lvl)
}

type Fields logger.Fields

func tracelog(msg string, f Fields) {
	logger.WithFields(logger.Fields(f)).Trace(msg)
}

func debuglog(msg string, f Fields) {
	logger.WithFields(logger.Fields(f)).Debug(msg)
}

func infolog(msg string, f Fields) {
	logger.WithFields(logger.Fields(f)).Info(msg)
}

func warnlog(msg string, f Fields) {
	logger.WithFields(logger.Fields(f)).Warn(msg)
}

func errorlog(msg string, f Fields) {
	logger.WithFields(logger.Fields(f)).Error(msg)
}

func paniclog(msg string, f Fields) {
	logger.WithFields(logger.Fields(f)).Panic(msg)
}
