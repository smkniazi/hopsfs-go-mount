# Copyright (c) Hopsworks AB. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.

#!/bin/bash

set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "Usage."
    echo "./docker_build.sh [image_prefix]"
    echo "  image_prefix - the prefix to be used with the docker image name."
    exit 0
fi

PREFIX=$1
USERID=`id -u`
GROUPID=`id -g`

command -v "docker"
if [[ "${?}" -ne 0 ]]; then
  echo "Make sure that you have docker installed to be able to build ePipe."
  exit 1
fi

VERSION=`grep VERSION Version.go | grep VERSION Version.go | sed 's/[\t A-Z"=]//g'`

rm -rf bin/*

DOCKER_IMAGE="hopsfs_mount_${VERSION}"
if [ "$PREFIX" != "" ]; then
  DOCKER_IMAGE="${PREFIX}"
fi

echo "Creating docker image ${DOCKER_IMAGE}"
docker build --build-arg userid=${USERID} --build-arg groupid=${GROUPID} . -t $DOCKER_IMAGE

echo "Building $platform using $DOCKER_IMAGE"
docker run --rm -v $DIR:/src -w /src --user hopsfs "$DOCKER_IMAGE" /bin/bash -l build 
