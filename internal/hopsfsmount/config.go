package hopsfsmount

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"bazil.org/fuse"
)

const (
	STAT_CACHE_TIME = 5 * time.Second
)

var StagingDir string = "/tmp"
var MntSrcDir string = "/"
var LogFile string = ""
var LogLevel string = "info"
var RootCABundle string = "/srv/hops/super_crypto/hdfs/hops_root_ca.pem"
var ClientCertificate string = "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem"
var ClientKey string = "/srv/hops/super_crypto/hdfs/hdfs_priv.pem"
var LazyMount bool = false
var AllowedPrefixesString string = "*"
var ReadOnly bool = false
var Tls bool = false
var Connectors int
var Version bool = false
var ForceOverrideUsername string = ""
var UseGroupFromHopsFsDatasetPath bool = false
var AllowOther bool = false
var HopfsProjectDatasetGroupRegex = regexp.MustCompile(`/*Projects/(?P<projectName>\w+)/(?P<datasetName>\w+)/\/*`)

func ParseArgsAndInitLogger(retryPolicy *RetryPolicy) {
	flag.BoolVar(&LazyMount, "lazy", false, "Allows to mount HopsFS filesystem before HopsFS is available")
	flag.DurationVar(&retryPolicy.TimeLimit, "retryTimeLimit", 5*time.Minute, "time limit for all retry attempts for failed operations")
	flag.IntVar(&retryPolicy.MaxAttempts, "retryMaxAttempts", 10, "Maxumum retry attempts for failed operations")
	flag.DurationVar(&retryPolicy.MinDelay, "retryMinDelay", 1*time.Second, "minimum delay between retries (note, first retry always happens immediatelly)")
	flag.DurationVar(&retryPolicy.MaxDelay, "retryMaxDelay", 60*time.Second, "maximum delay between retries")
	flag.StringVar(&AllowedPrefixesString, "allowedPrefixes", "*", "Comma-separated list of allowed path prefixes on the remote file system, if specified the mount point will expose access to those prefixes only")
	flag.BoolVar(&ReadOnly, "readOnly", false, "Enables mount with readonly")
	flag.StringVar(&LogLevel, "logLevel", "info", "logs to be printed. error, warn, info, debug, trace")
	flag.StringVar(&StagingDir, "stageDir", "/tmp", "stage directory for writing files")
	flag.BoolVar(&Tls, "tls", false, "Enables tls connections")
	flag.StringVar(&RootCABundle, "rootCABundle", "/srv/hops/super_crypto/hdfs/hops_root_ca.pem", "Root CA bundle location ")
	flag.StringVar(&ClientCertificate, "clientCertificate", "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem", "Client certificate location")
	flag.StringVar(&ClientKey, "clientKey", "/srv/hops/super_crypto/hdfs/hdfs_priv.pem", "Client key location")
	flag.StringVar(&MntSrcDir, "srcDir", "/", "HopsFS src directory")
	flag.StringVar(&LogFile, "logFile", "", "Log file path. By default the log is written to console")
	flag.IntVar(&Connectors, "numConnections", 1, "Number of connections with the namenode")
	flag.StringVar(&ForceOverrideUsername, "hopsFSUserName", "", " username")
	flag.BoolVar(&UseGroupFromHopsFsDatasetPath, "getGroupFromHopsFSDatasetPath", false, "Get the group from hopsfs dataset path. This will work if a hopsworks project is mounted")
	flag.BoolVar(&AllowOther, "allowOther", true, "Allow other users to use the filesystem")
	flag.BoolVar(&Version, "version", false, "Print version")

	flag.Usage = usage
	flag.Parse()

	if Version {
		fmt.Printf("Version: %s\n", VERSION)
		fmt.Printf("Git commit: %s\n", GITCOMMIT)
		fmt.Printf("Date: %s\n", BUILDTIME)
		fmt.Printf("Host: %s\n", HOSTNAME)
		os.Exit(0)
	}

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}

	if err := checkLogFileCreation(); err != nil {
		log.Fatalf("Error creating log file. Error: %v", err)
	}
	initLogger(LogLevel, false, LogFile)

	Loginfo(fmt.Sprintf("Staging dir is:%s, Using TLS: %v, RetryAttempts: %d,  LogFile: %s", StagingDir, Tls, retryPolicy.MaxAttempts, LogFile), nil)
	Loginfo(fmt.Sprintf("hopsfs-mount: current head GITCommit: %s Built time: %s Built by: %s ", GITCOMMIT, BUILDTIME, HOSTNAME), nil)
}

// check that we can create / open the log file
func checkLogFileCreation() error {
	if LogFile != "" {
		if _, err := os.Stat(LogFile); err == nil {
			// file exists. check if it is writeable
			if f, err := os.OpenFile(LogFile, os.O_RDWR|os.O_APPEND, 0600); err != nil {
				return err
			} else {
				f.Close()
			}
		} else if os.IsNotExist(err) {
			// check if we can create the log file
			if f, err := os.OpenFile(LogFile, os.O_RDWR|os.O_CREATE, 0600); err != nil {
				return err
			} else {
				f.Close()
			}
		} else {
			// Schrodinger: file may or may not exist. See err for details.
			return err
		}
	}
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [Options] Namenode:Port MountPoint\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  \nOptions:\n")
	flag.PrintDefaults()
}

func GetMountOptions(ro bool) []fuse.MountOption {
	mountOptions := []fuse.MountOption{fuse.FSName("hopsfs"),
		fuse.Subtype("hopsfs"),
		// fuse.WritebackCache(), // write to kernel cache, improves performance for small writes.
		// NOTE: It creates problem when reading file updated by external clients
		// https://www.kernel.org/doc/Documentation/filesystems/fuse-io.txt
		fuse.MaxReadahead(1024 * 64), //TODO: make configurable
		fuse.DefaultPermissions(),
	}

	if AllowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}

	if ro {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}
	return mountOptions
}
