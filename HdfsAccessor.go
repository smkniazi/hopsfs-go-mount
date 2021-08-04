// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"time"

	"bazil.org/fuse"
	"github.com/colinmarc/hdfs/v2"
)

const (
	UGCacheTime = 3 * time.Second
)

// Interface for accessing HDFS
// Concurrency: thread safe: handles unlimited number of concurrent requests
type HdfsAccessor interface {
	OpenRead(path string) (ReadSeekCloser, error) // Opens HDFS file for reading
	CreateFile(path string,
		mode os.FileMode, overwrite bool) (HdfsWriter, error) // Opens HDFS file for writing
	ReadDir(path string) ([]Attrs, error)         // Enumerates HDFS directory
	Stat(path string) (Attrs, error)              // Retrieves file/directory attributes
	StatFs() (FsInfo, error)                      // Retrieves HDFS usage
	Mkdir(path string, mode os.FileMode) error    // Creates a directory
	Remove(path string) error                     // Removes a file or directory
	Rename(oldPath string, newPath string) error  // Renames a file or directory
	EnsureConnected() error                       // Ensures HDFS accessor is connected to the HDFS name node
	Chown(path string, owner, group string) error // Changes the owner and group of the file
	Chmod(path string, mode os.FileMode) error    // Changes the mode of the file
	Close() error                                 // Close current meta connection if needed
}

type TLSConfig struct {
	TLS bool // enable/disable using tls
	// if TLS is set then also set the following parameters
	RootCABundle      string
	ClientCertificate string
	ClientKey         string
}

type hdfsAccessorImpl struct {
	Clock               Clock                   // interface to get wall clock time
	NameNodeAddresses   []string                // array of Address:port string for the name nodes
	MetadataClient      *hdfs.Client            // HDFS client used for metadata operations
	MetadataClientMutex sync.Mutex              // Serializing all metadata operations for simplicity (for now), TODO: allow N concurrent operations
	UserNameToUidCache  map[string]UGCacheEntry // cache for converting usernames to UIDs
	GroupNameToUidCache map[string]UGCacheEntry // cache for converting usernames to UIDs
	TLSConfig           TLSConfig               // enable/disable using tls
}

type UGCacheEntry struct {
	ID      uint32    // User/Group Id
	Expires time.Time // Absolute time when this cache entry expires
}

var _ HdfsAccessor = (*hdfsAccessorImpl)(nil) // ensure hdfsAccessorImpl implements HdfsAccessor

// Creates an instance of HdfsAccessor
func NewHdfsAccessor(nameNodeAddresses string, clock Clock, tlsConfig TLSConfig) (HdfsAccessor, error) {
	nns := strings.Split(nameNodeAddresses, ",")

	this := &hdfsAccessorImpl{
		NameNodeAddresses:   nns,
		Clock:               clock,
		UserNameToUidCache:  make(map[string]UGCacheEntry),
		GroupNameToUidCache: make(map[string]UGCacheEntry),
		TLSConfig:           tlsConfig,
	}
	return this, nil
}

// Ensures that metadata client is connected
func (dfs *hdfsAccessorImpl) EnsureConnected() error {
	if dfs.MetadataClient != nil {
		return nil
	}
	return dfs.ConnectMetadataClient()
}

// Establishes connection to the name node (assigns MetadataClient field)
func (dfs *hdfsAccessorImpl) ConnectMetadataClient() error {
	client, err := dfs.ConnectToNameNode()
	if err != nil {
		return err
	}
	dfs.MetadataClient = client
	return nil
}

// Establishes connection to a name node in the context of some other operation
func (dfs *hdfsAccessorImpl) ConnectToNameNode() (*hdfs.Client, error) {
	// connecting to HDFS name node
	client, err := dfs.connectToNameNodeImpl()
	if err != nil {
		// Connection failed
		return nil, errors.New(fmt.Sprintf("Fail to connect to name node with error: %s", err.Error()))
	}
	return client, nil
}

// Performs an attempt to connect to the HDFS name
func (dfs *hdfsAccessorImpl) connectToNameNodeImpl() (*hdfs.Client, error) {
	hadoopUser := os.Getenv("HADOOP_USER_NAME")
	if hadoopUser == "" {
		u, err := user.Current()
		if err != nil {
			return nil, fmt.Errorf("Couldn't determine user: %s", err)
		}
		hadoopUser = u.Username
	}
	loginfo(fmt.Sprintf("Connecting as user: %s", hadoopUser), nil)

	// Performing an attempt to connect to the name node
	// Colinmar's hdfs implementation has supported the multiple name node connection
	hdfsOptions := hdfs.ClientOptions{
		Addresses: dfs.NameNodeAddresses,
		TLS:       dfs.TLSConfig.TLS,
		User:      hadoopUser,
	}

	if dfs.TLSConfig.TLS {
		hdfsOptions.RootCABundle = dfs.TLSConfig.RootCABundle
		hdfsOptions.ClientKey = dfs.TLSConfig.ClientKey
		hdfsOptions.ClientCertificate = dfs.TLSConfig.ClientCertificate
	}

	client, err := hdfs.NewClient(hdfsOptions)
	if err != nil {
		return nil, err
	}
	// connection is OK, but we need to check whether name node is operating ans expected
	// (this also checks whether name node is Active)
	// Performing this check, by doing Stat() for a path inside root directory
	// Note: The file '/$' doesn't have to be present
	// - both nil and ErrNotExists error codes indicate success of the operation
	_, statErr := client.Stat("/")

	if statErr == nil {
		// Succesfully connected
		return client, nil
	} else {
		client.Close()
		logerror(fmt.Sprintf("Faild to connect to NN. Error: %v ", statErr), nil)
		return nil, statErr
	}
}

// Opens HDFS file for reading
func (dfs *hdfsAccessorImpl) OpenRead(path string) (ReadSeekCloser, error) {
	// Blocking read. This is to reduce the connections pressue on hadoop-name-node
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	reader, err := dfs.MetadataClient.Open(path)
	if err != nil {
		return nil, err
	}
	return NewHdfsReader(reader), nil
}

// Creates new HDFS file
func (dfs *hdfsAccessorImpl) CreateFile(path string, mode os.FileMode, overwrite bool) (HdfsWriter, error) {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	writer, err := dfs.MetadataClient.CreateFile(path, 3, 64*1024*1024, mode, overwrite)
	if err != nil {
		return nil, err
	}

	return NewHdfsWriter(writer), nil
}

// Enumerates HDFS directory
func (dfs *hdfsAccessorImpl) ReadDir(path string) ([]Attrs, error) {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	files, err := dfs.MetadataClient.ReadDir(path)
	if err != nil {
		if IsSuccessOrBenignError(err) {
			// benign error (e.g. path not found)
			return nil, err
		}
		// We've got error from this client, setting to nil, so we try another one next time
		dfs.MetadataClient = nil
		// TODO: attempt to gracefully close the conenction
		return nil, err
	}
	allAttrs := make([]Attrs, len(files))
	for i, fileInfo := range files {
		allAttrs[i] = dfs.AttrsFromFileInfo(fileInfo)
	}
	return allAttrs, nil
}

// Retrieves file/directory attributes
func (dfs *hdfsAccessorImpl) Stat(path string) (Attrs, error) {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return Attrs{}, err
		}
	}

	fileInfo, err := dfs.MetadataClient.Stat(path)
	if err != nil {
		if IsSuccessOrBenignError(err) {
			// benign error (e.g. path not found)
			return Attrs{}, err
		}
		// We've got error from this client, setting to nil, so we try another one next time
		dfs.MetadataClient = nil
		// TODO: attempt to gracefully close the conenction
		return Attrs{}, err
	}
	return dfs.AttrsFromFileInfo(fileInfo), nil
}

// Retrieves HDFS usages
func (dfs *hdfsAccessorImpl) StatFs() (FsInfo, error) {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return FsInfo{}, err
		}
	}

	fsInfo, err := dfs.MetadataClient.StatFs()
	if err != nil {
		if IsSuccessOrBenignError(err) {
			return FsInfo{}, err
		}
		dfs.MetadataClient = nil
		return FsInfo{}, err
	}
	return dfs.AttrsFromFsInfo(fsInfo), nil
}

// Converts os.FileInfo + underlying proto-buf data into Attrs structure
func (dfs *hdfsAccessorImpl) AttrsFromFileInfo(fileInfo os.FileInfo) Attrs {
	// protoBufDatr := fileInfo.Sys().(*hadoop_hdfs.HdfsFileStatusProto)
	fi := fileInfo.(*hdfs.FileInfo)
	mode := os.FileMode(fi.Permission())
	if fileInfo.IsDir() {
		mode |= os.ModeDir
	}

	modificationTime := time.Unix(int64(fi.ModificationTime())/1000, 0)
	return Attrs{
		Inode:  fi.FileId(),
		Name:   fileInfo.Name(),
		Mode:   mode,
		Size:   fi.Length(),
		Uid:    dfs.LookupUid(fi.Owner()),
		Mtime:  modificationTime,
		Ctime:  modificationTime,
		Crtime: modificationTime,
		Gid:    dfs.LookupGid(fi.OwnerGroup())}
}

func (dfs *hdfsAccessorImpl) AttrsFromFsInfo(fsInfo hdfs.FsInfo) FsInfo {
	return FsInfo{
		capacity:  fsInfo.Capacity,
		used:      fsInfo.Used,
		remaining: fsInfo.Remaining}
}

func HadoopTimestampToTime(timestamp uint64) time.Time {
	return time.Unix(int64(timestamp)/1000, 0)
}

// Performs a cache-assisted lookup of UID by username
func (dfs *hdfsAccessorImpl) LookupUid(userName string) uint32 {
	if userName == "" {
		return 0
	}
	// Note: this method is called under MetadataClientMutex, so accessing the cache dirctionary is safe
	cacheEntry, ok := dfs.UserNameToUidCache[userName]
	if ok && dfs.Clock.Now().Before(cacheEntry.Expires) {
		return cacheEntry.ID
	}

	u, err := user.Lookup(userName)
	if u != nil {
		var uid64 uint64
		if err == nil {
			// UID is returned as string, need to parse it
			uid64, err = strconv.ParseUint(u.Uid, 10, 32)
		}
		if err != nil {
			uid64 = (1 << 31) - 1
		}
		dfs.UserNameToUidCache[userName] = UGCacheEntry{
			ID:      uint32(uid64),
			Expires: dfs.Clock.Now().Add(UGCacheTime)}
		return uint32(uid64)

	} else {
		return 0
	}
}

// Performs a cache-assisted lookup of GID by grooupname
func (dfs *hdfsAccessorImpl) LookupGid(groupName string) uint32 {
	if groupName == "" {
		return 0
	}
	// Note: this method is called under MetadataClientMutex, so accessing the cache dirctionary is safe
	cacheEntry, ok := dfs.GroupNameToUidCache[groupName]
	if ok && dfs.Clock.Now().Before(cacheEntry.Expires) {
		return cacheEntry.ID
	}

	g, err := user.LookupGroup(groupName)
	if g != nil {
		var gid64 uint64
		if err == nil {
			// GID is returned as string, need to parse it
			gid64, err = strconv.ParseUint(g.Gid, 10, 32)
		}
		if err != nil {
			gid64 = (1 << 31) - 1
		}
		dfs.GroupNameToUidCache[groupName] = UGCacheEntry{
			ID:      uint32(gid64),
			Expires: dfs.Clock.Now().Add(UGCacheTime)}
		return uint32(gid64)

	} else {
		logwarn(fmt.Sprintf("Group not found %s", groupName), nil)
		return 0
	}
}

// Returns true if err==nil or err is expected (benign) error which should be propagated directoy to the caller
func IsSuccessOrBenignError(err error) bool {
	if err == nil || err == io.EOF || err == fuse.EEXIST {
		return true
	}
	if pathError, ok := err.(*os.PathError); ok && (pathError.Err == os.ErrNotExist || pathError.Err == os.ErrPermission) {
		return true
	} else {
		return false
	}
}

// Creates a directory
func (dfs *hdfsAccessorImpl) Mkdir(path string, mode os.FileMode) error {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	err := dfs.MetadataClient.Mkdir(path, mode)
	if err != nil {
		if strings.HasSuffix(err.Error(), "file already exists") {
			err = fuse.EEXIST
		}
	}
	return err
}

// Removes file or directory
func (dfs *hdfsAccessorImpl) Remove(path string) error {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Remove(path)
}

// Renames file or directory
func (dfs *hdfsAccessorImpl) Rename(oldPath string, newPath string) error {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Rename(oldPath, newPath)
}

// Changes the mode of the file
func (dfs *hdfsAccessorImpl) Chmod(path string, mode os.FileMode) error {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Chmod(path, mode)
}

// Changes the owner and group of the file
func (dfs *hdfsAccessorImpl) Chown(path string, user, group string) error {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()
	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Chown(path, user, group)
}

// Close current connection if needed
func (dfs *hdfsAccessorImpl) Close() error {
	dfs.MetadataClientMutex.Lock()
	defer dfs.MetadataClientMutex.Unlock()

	if dfs.MetadataClient != nil {
		err := dfs.MetadataClient.Close()
		dfs.MetadataClient = nil
		return err
	}
	return nil
}
