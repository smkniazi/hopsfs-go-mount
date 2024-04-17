// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package ugcache

import (
	"fmt"
	"os/user"
	"strconv"
	"sync"
	"syscall"
	"time"

	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

const (
	UGCacheTime = 3 * time.Second
)

type ugID struct {
	id      uint32    // User/Group Id
	expires time.Time // Absolute time when this cache entry expires
}

type ugName struct {
	name    string    // User/Group name
	expires time.Time // Absolute time when this cache entry expires
}

var userNameToUidCache = make(map[string]ugID)  // cache for converting usernames to UIDs
var groupNameToUidCache = make(map[string]ugID) // cache for converting usernames to UIDs

var userIdToNameCache = make(map[uint32]ugName)  // cache for converting usernames to UIDs
var groupIdToNameCache = make(map[uint32]ugName) // cache for converting usernames to UIDs

var ugMutex sync.Mutex

func LookupUId(userName string) uint32 {
	lockUGCache()
	defer unlockUGCache()

	if userName == "" {
		return 0
	}
	// Note: this method is called under MetadataClientMutex, so accessing the cache dirctionary is safe
	cacheEntry, ok := userNameToUidCache[userName]
	if ok && time.Now().Before(cacheEntry.expires) {
		return cacheEntry.id
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
		userNameToUidCache[userName] = ugID{
			id:      uint32(uid64),
			expires: time.Now().Add(UGCacheTime)}
		return uint32(uid64)

	} else {
		return 0
	}
}

func LookupGid(groupName string) uint32 {
	lockUGCache()
	defer unlockUGCache()

	if groupName == "" {
		return 0
	}
	// Note: this method is called under MetadataClientMutex, so accessing the cache dictionary is safe
	cacheEntry, ok := groupNameToUidCache[groupName]
	if ok && time.Now().Before(cacheEntry.expires) {
		return cacheEntry.id
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
		groupNameToUidCache[groupName] = ugID{
			id:      uint32(gid64),
			expires: time.Now().Add(UGCacheTime)}
		return uint32(gid64)

	} else {
		return 0
	}
}

func LookupUserName(uid uint32) string {
	lockUGCache()
	defer unlockUGCache()

	cacheEntry, ok := userIdToNameCache[uid]
	if ok && time.Now().Before(cacheEntry.expires) {
		return cacheEntry.name
	}

	u, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
	if err != nil {
		return ""
	}
	userIdToNameCache[uid] = ugName{
		name:    u.Username,
		expires: time.Now().Add(UGCacheTime)}
	return u.Username
}

func LookupGroupName(gid uint32) string {
	lockUGCache()
	defer unlockUGCache()

	cacheEntry, ok := groupIdToNameCache[gid]
	if ok && time.Now().Before(cacheEntry.expires) {
		return cacheEntry.name
	}

	g, err := user.LookupGroupId(strconv.FormatUint(uint64(gid), 10))
	if err != nil {
		return ""
	}
	groupIdToNameCache[gid] = ugName{
		name:    g.Name,
		expires: time.Now().Add(UGCacheTime)}
	return g.Name
}

func CurrentUserName() (string, error) {
	u, err := user.Current()
	if err != nil {
		logger.Error("Couldn't determine current user", logger.Fields{"Error": err})
		return "", syscall.EPERM
	}
	return u.Username, nil
}

func lockUGCache() {
	ugMutex.Lock()
}

func unlockUGCache() {
	ugMutex.Unlock()
}

func GetFilesystemOwner(dfsOwner string, defaultOwner string) uint32 {
	if defaultOwner != "root" {
		return LookupUId(defaultOwner)
	}
	return LookupGid(dfsOwner)
}

func GetFilesystemOwnerGroup(dfsGroup string, defaultGroup string) uint32 {
	if defaultGroup != "root" {
		return LookupGid(defaultGroup)
	}
	return LookupGid(dfsGroup)
}

func GetHadoopUid(hadoopUserName string) uint32 {
	var hadoopUserID uint32
	hadoopUserID = LookupUId(hadoopUserName)
	if hadoopUserName != "root" && hadoopUserID == 0 {
		logger.Warn(fmt.Sprintf("Unable to find user id for user: %s, returning uid: 0", hadoopUserName), nil)

	}
	return hadoopUserID
}
