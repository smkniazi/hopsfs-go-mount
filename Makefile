# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.


GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`

all: hopsfs-mount 

hopsfs-mount: *.go 
	go build -tags osusergo,netgo -ldflags="-w -X main.GITCOMMIT=${GITCOMMIT} -X main.BUILDTIME=${BUILDTIME} -X main.HOSTNAME=${HOSTNAME}" -o hopsfs-mount

clean:
	rm -f hopsfs-mount \

mock_%_test.go: %.go 
	mockgen -source $< -package main  -self_package=logicalclocks.com/hopsfs-mount > $@~
	mv -f $@~ $@

mock: hopsfs-mount \
	mock_HdfsAccessor_test.go \
	mock_ReadSeekCloser_test.go \
	mock_HdfsWriter_test.go

test: mock 
	go test -v -coverprofile coverage.txt -covermode atomic
