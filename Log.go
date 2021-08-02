// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"io"
	"runtime"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logger "github.com/sirupsen/logrus"
)

// bunch of constants for logging
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
	Offset            = "offset"
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
	Line              = "line"
	ReqOffset         = "req_offset"
)

var ReportCaller = true

func init() {
	logger.SetLevel(logger.ErrorLevel)
}

func initLogger(l string, out io.Writer, reportCaller bool) {
	ReportCaller = reportCaller
	lvl, err := logger.ParseLevel(l)
	if err != nil {
		logger.Errorf("Invlid log level %s ", l)
		lvl = logger.ErrorLevel
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
		FieldsOrder:    []string{Operation, Path, Bytes, TotalBytesRead, TotalBytesWritten},
	})

	// Only log the warning severity or above.
	logger.SetLevel(lvl)
}

type Fields logger.Fields

func logtrace(msg string, f Fields) {
	logmessae(logger.TraceLevel, msg, f)
}

func logdebug(msg string, f Fields) {
	logmessae(logger.DebugLevel, msg, f)
}

func loginfo(msg string, f Fields) {
	logmessae(logger.InfoLevel, msg, f)
}

func logwarn(msg string, f Fields) {
	logmessae(logger.WarnLevel, msg, f)
}

func logerror(msg string, f Fields) {
	logmessae(logger.ErrorLevel, msg, f)
}

func logfatal(msg string, f Fields) {
	logmessae(logger.FatalLevel, msg, f)
}

func logpanic(msg string, f Fields) {
	logmessae(logger.PanicLevel, msg, f)
}

func logmessae(lvl logger.Level, msg string, f Fields) {
	if ReportCaller {
		_, file, line, _ := runtime.Caller(2)
		if f == nil {
			f = Fields{}
		}
		f[Line] = fmt.Sprintf("%s:%d", file, line)
	}

	switch lvl {
	case logger.PanicLevel:
		logger.WithFields(logger.Fields(f)).Panic(msg)
	case logger.FatalLevel:
		logger.WithFields(logger.Fields(f)).Fatal(msg)
	case logger.ErrorLevel:
		logger.WithFields(logger.Fields(f)).Error(msg)
	case logger.WarnLevel:
		logger.WithFields(logger.Fields(f)).Warn(msg)
	case logger.InfoLevel:
		logger.WithFields(logger.Fields(f)).Info(msg)
	case logger.DebugLevel:
		logger.WithFields(logger.Fields(f)).Debug(msg)
	case logger.TraceLevel:
		logger.WithFields(logger.Fields(f)).Trace(msg)
	default:
		logger.WithFields(logger.Fields(f)).Info(msg)
	}
}
