module logicalclocks.com/hopsfs-mount

go 1.15

require (
	bazil.org/fuse v0.0.0-20200524192727-fb710f7dfd05
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/golang/mock v1.6.0
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/stretchr/testify v1.7.0
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e
)

replace github.com/colinmarc/hdfs/v2 v2.2.0 => github.com/logicalclocks/hopsfs-go-client/v2 v2.3.0

// replace github.com/colinmarc/hdfs/v2 v2.2.0 => ../hopsfs-go-client
