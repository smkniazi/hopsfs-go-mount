// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package hopsfsmount

import (
	"errors"
	"fmt"
	"time"

	"bazil.org/fuse"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/ugcache"
)

func ChmodOp(attrs *Attrs, fileSystem *FileSystem, path string, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	Loginfo("Setting attributes", Fields{Operation: Chmod, Path: path, Mode: req.Mode})
	err := fileSystem.getDFSConnector().Chmod(path, req.Mode)
	if err != nil {
		return err
	} else {
		attrs.Mode = req.Mode
		resp.Attr.Mode = req.Mode
		return nil
	}
}

func SetAttrChownOp(attrs *Attrs, fileSystem *FileSystem, path string, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	var uid = attrs.Uid
	var gid = attrs.Gid

	if req.Valid.Uid() {
		uid = req.Uid
	}

	if req.Valid.Gid() {
		gid = req.Gid
	}

	return ChownOp(attrs, fileSystem, path, uid, gid)
}

func ChownOp(attrs *Attrs, fileSystem *FileSystem, path string, uid uint32, gid uint32) error {
	var userName = ""
	var groupName = ""

	if ForceOverrideUsername != "" {
		userName = ForceOverrideUsername
	} else {
		userName = ugcache.LookupUserName(uid)
		if userName == "" {
			return fmt.Errorf(fmt.Sprintf("Setattr failed. Unable to find user information. Path %s", path))
		}
	}

	if UseGroupFromHopsFsDatasetPath {
		pathGroupName, err := getGroupNameFromPath(path)
		if err == nil {
			groupName = pathGroupName
		} else {
			Logwarn(err.Error(), Fields{Path: path})
		}
	} else {
		groupName = ugcache.LookupGroupName(gid)
	}

	if groupName == "" {
		return fmt.Errorf(fmt.Sprintf("Setattr failed. Unable to find group information. Path %s", path))
	}

	Loginfo("Setting attributes", Fields{Operation: Chown, Path: path, UID: uid, User: userName, GID: gid, Group: groupName})
	err := fileSystem.getDFSConnector().Chown(path, userName, groupName)

	if err != nil {
		return err
	} else {
		attrs.Uid = uid
		attrs.Gid = gid
		return nil
	}
}

func UpdateTS(attrs *Attrs, fileSystem *FileSystem, path string, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	// in future if we need access time then we can update the file system client to support it
	if req.Valid.Atime() {
		Logdebug("The stat op in hopsfs client returns os.FileInfo which does not have access time. Ignoring atime for now", nil)
	}

	if req.Valid.Mtime() {
		attrs.Mtime = time.Unix(int64(req.Mtime.Second()), 0)
	}

	if req.Valid.Handle() {
		Logwarn("Setattr Handle is not implemented yet.", nil)
	}

	if req.Valid.AtimeNow() {
		Logdebug("Setattr AtimeNow is not implemented yet.", nil)
	}

	if req.Valid.MtimeNow() {
		Logdebug("Setattr MtimeNow is not implemented yet.", nil)
	}

	if req.Valid.LockOwner() {
		Logwarn("Setattr LockOwner is not implemented yet.", nil)
	}

	return nil
}

func getGroupNameFromPath(path string) (string, error) {
	Loginfo("Getting group name from path", Fields{Path: path})
	result := HopfsProjectDatasetGroupRegex.FindAllStringSubmatch(path, -1)
	if len(result) == 0 {
		return "", errors.New("could not get project name and dataset name from path " + path)
	}

	return result[0][1] + "__" + result[0][2], nil
}
