package main

import (
	"flag"

	"sync"

	"github.com/DurantVivado/reedsolomon"
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
	diskPath  string //the disk path
	available bool   //it's flag and when disk fails, it renders false.
	numBlocks int    //it tells how many blocks a disk holds
	capacity  int64  //the capacity of a disk
}

type Erasure struct {
	K               int                       `json:"dataShards"`   // the number of data blocks in a stripe
	M               int                       `json:"parityShards"` // the number of parity blocks in a stripe
	BlockSize       int64                     `json:"blockSize"`    // the block size. default to 4KiB
	FileMeta        []*FileInfo               `json:"FileLists"`
	conStripes      int                       //how many stripes are allowed to encode/decode concurrently
	replicateFactor int                       // the replication factor for config file
	sEnc            reedsolomon.StreamEncoder // the reedsolomon streaming encoder, for streaming access
	enc             reedsolomon.Encoder       // the reedsolomon encoder, for block access
	dataStripeSize  int64                     //the data stripe size, equal to k*bs
	allStripeSize   int64                     //the data plus parity stripe size, equal to (k+m)*bs
	diskInfos       []*DiskInfo               // disk paths
	configFile      string                    // configure file
	fileMap         map[string]*FileInfo      // File info lists
	diskFilePath    string                    // the path of file recording all disks path
	dataBlobPool    sync.Pool                 // memory pool for conStripes data  access
	allBlobPool     sync.Pool                 // memory pool for conStripes stripe access
	errgroupPool    sync.Pool                 //errgroup pool
	blockPool       sync.Pool                 //the pool for block-size access
}
type FileInfo struct {
	FileName      string  `json:"fileName"` //file name
	FileSize      int64   `json:"fileSize"` //file size
	Hash          string  `json:"fileHash"` //hash value (SHA256 by default)
	Distribution  [][]int `json:"fileDist"` //distribution forms a block->disk mapping
	blockToOffset [][]int //blockToOffset has the same row and column number as Distribution but points to the block offset relative to a disk.

	// metaInfo     *os.FileInfo //system-level file info
}

//global CLI parameters
var (
	blockSize         int64
	mode              string
	k                 int
	m                 int
	filePath          string
	savePath          string
	newFilePath       string
	new_k             int
	new_m             int
	recoveredDiskPath string
	failMode          string
	failNum           int
	override          bool
	conWrites         bool
	conReads          bool
	conStripes        int
	replicateFactor   int
)

//the parameter lists, with fullname or abbreviation
func flag_init() {
	flag.Int64Var(&blockSize, "bs", 4096, "the block size in bytes")
	flag.Int64Var(&blockSize, "blockSize", 4096, "the block size in bytes")

	flag.StringVar(&mode, "md", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")
	flag.StringVar(&mode, "mode", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")

	flag.IntVar(&k, "k", 12, "the number of data shards(<256)")
	flag.IntVar(&k, "dataNum", 12, "the number of data shards(<256)")

	flag.IntVar(&m, "m", 4, "the number of parity shards(2-4)")
	flag.IntVar(&m, "parityNum", 4, "the number of parity shards(2-4)")

	flag.StringVar(&filePath, "f", "", "upload: the local file path, download&update: the remote file name")
	flag.StringVar(&filePath, "filePath", "", "upload: the local file path, download&update: the remote file name")

	flag.StringVar(&savePath, "sp", "file.save", "the local saving path(local path)")
	flag.StringVar(&savePath, "savePath", "file.save", "the local saving path(local path)")

	flag.IntVar(&new_k, "new_k", 32, "the new number of data shards(<256)")
	flag.IntVar(&new_k, "newDataNum", 32, "the new number of data shards(<256)")

	flag.IntVar(&new_m, "new_m", 8, "the new number of parity shards(2-4)")
	flag.IntVar(&new_m, "newParityNum", 8, "the new number of parity shards(2-4)")

	flag.StringVar(&recoveredDiskPath, "rDP", "/tmp/restore", "the data path for recovered disk, default to /tmp/data")
	flag.StringVar(&recoveredDiskPath, "recoverDiskPath", "/tmp/restore", "the data path for recovered disk, default to /tmp/data")

	flag.BoolVar(&override, "o", false, "whether to override former files or directories, default to false")
	flag.BoolVar(&override, "override", false, "whether to override former files or directories, default to false")

	flag.BoolVar(&conWrites, "cw", true, "whether to enable concurrent write, default is false")
	flag.BoolVar(&conWrites, "conWrites", true, "whether to enable concurrent write, default is false")

	flag.BoolVar(&conReads, "cr", true, "whether to enable concurrent read, default is false")
	flag.BoolVar(&conReads, "conReads", true, "whether to enable concurrent read, default is false")

	flag.StringVar(&failMode, "fmd", "diskFail", "simulate [diskFail] or [bitRot] mode")
	flag.StringVar(&failMode, "failMode", "diskFail", "simulate [diskFail] or [bitRot] mode")

	flag.IntVar(&failNum, "fn", 0, "simulate multiple disk failure, provides the fail number of disks")
	flag.IntVar(&failNum, "failNum", 0, "simulate multiple disk failure, provides the fail number of disks")

	flag.IntVar(&replicateFactor, "rf", 3, "the meta data is replicated `rf`- fold to provide enough reliability, default is 3-fold")
	flag.IntVar(&replicateFactor, "replicateFactor", 3, "the meta data is replicated `rf`- fold to provide enough reliability, default is 3-fold")

	flag.IntVar(&conStripes, "cs", 100, "how many stripes are allowed to encode/decode concurrently")
	flag.IntVar(&conStripes, "conStripes", 100, "how many stripes are allowed to encode/decode concurrently")
}

//global system-level variables
var (
	wg      sync.WaitGroup
	err     error
	erasure = Erasure{
		configFile:   "conf.json",
		fileMap:      make(map[string]*FileInfo),
		diskFilePath: ".hdr.disks.path",
	}
)

//constant variables
const (
	tempFile      = "./test/file.temp"
	maxGoroutines = 10240
)

//templates
const (
	config_templ = `
This file is automatically generated, DO NOT EDIT
System-level  Parameters:
dataShards(k): {.k}
dataShards(k): {.m}
blockSize(bytes): {.blockSize}
	`

	file_templ = `
FileName: {{.fileName}}
Size(bytes): {{.fileSize}}
SHA256SUM: {{.hash}}
Distribution: {{.distribution}}
`
)
