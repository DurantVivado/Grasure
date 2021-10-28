package main

import (
	"flag"
	"sync"

	"github.com/klauspost/reedsolomon"
)

type CustomerAPI interface {
	Read(filename string) ([]byte, error)
	Write(filename string) (bool, error)
	ReadAll(filename []string) ([][]byte, error)
	WriteAll(filename []string) (bool, error)
	Delete(filename string) (bool, error)
	Change(filename string) (bool, error) //change file's meta
}
type HDRInfo struct {
	used  uint64
	free  uint64
	total uint64
}

type DiskInfo struct {
	diskPath  string
	available bool
}

type Erasure struct {
	k            int                  // the number of data blocks in a stripe
	m            int                  // the number of parity blocks in a stripe
	enc          reedsolomon.Encoder  // the reedsolomon encoder
	blockSize    int64                //the block size. default to 4KiB
	diskInfos    []*DiskInfo          //disk paths
	configFile   string               //configure file
	fileMap      map[string]*FileInfo //File info lists
	rwmu         sync.RWMutex         //read write mutex
	diskFilePath string               //the path of file recording all disks path
}
type FileInfo struct {
	fileName string //file name
	fileSize int64  //file size
	// metaInfo     *os.FileInfo //system-level file info
	hash         string //hash value (SHA256 by default)
	distribution []int  //distribution represents the block replacement respect to disks
}

//the parameter lists
var blockSize = flag.Int("blockSize", 4096, "the block size in bytes")
var mode = flag.String("mode", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")
var k = flag.Int("k", 12, "the number of data shards(<256)")
var m = flag.Int("m", 4, "the number of parity shards(2-4)")
var diskPath = flag.String("diskPath", "", "the disks path")
var file = flag.String("file", "", "upload: the local file path, download&update: the remote file name")
var newFile = flag.String("newFile", "", "the updated file path(local path)")
var savePath = flag.String("savePath", "file.save", "the local saving path(local path) for file")
var new_k = flag.Int("new_k", 16, "the new number of data shards(<256)")
var new_m = flag.Int("new_m", 4, "the new number of parity shards(2-4)")
var recoveredDiskPath = flag.String("recoveredDiskPath", "/tmp/data", "the data path for recovered disk, default to /tmp/data")
var failMode = flag.String("failMode", "diskFail", "simulate diskFail or bitRot mode")
var failNum = flag.Int("failNum", 0, "simulate multiple disk failure, provides the fail number of disks")
var override = flag.Bool("override", false, "whether to override former files or directories")

var err error

//global variables
var erasure = Erasure{
	configFile:   ".hdr.sys",
	fileMap:      make(map[string]*FileInfo),
	diskFilePath: ".hdr.disks.path",
}
