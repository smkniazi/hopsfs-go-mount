# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.


GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`

all: hdfs-mount 

hdfs-mount: *.go 
	go build -ldflags="-w -X main.GITCOMMIT=${GITCOMMIT} -X main.BUILDTIME=${BUILDTIME} -X main.HOSTNAME=${HOSTNAME}" -o hopsfs-mount

clean:
	rm -f hopsfs-mount 
