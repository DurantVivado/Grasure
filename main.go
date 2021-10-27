/*
 * @Author: your name
 * @Date: 2021-09-06 04:47:11
 * @LastEditTime: 2021-09-06 04:47:11
 * @LastEditors: Please set LastEditors
 * @Description: We decide to consider every operation in concurrent manner
 * @ProjectUrl: github.com/DurantVivado/Grasure
 */
package main

import (
	"flag"
	"log"
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
	blockSize    int                  //the block size. default to 4KiB
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

func main() {
	//We read each file and make byte flow
	flag.Parse()
	//We read the config file

	switch *mode {
	case "init":
		erasure.init()
	case "read":
		//read a file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath()
		failOnErr(*mode, err)
		// erasure.destroy(*failMode, *failNum)
		err = erasure.read(*file, *savePath)
		failOnErr(*mode, err)
	case "encode":
		//We are entering the encoding mode, and for brevity,we only encode one file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath()
		failOnErr(*mode, err)
		_, err := erasure.encode(*file)
		failOnErr(*mode, err)
		err = erasure.writeConfig()
		failOnErr(*mode, err)
	case "update":
		//update an old file according to a new file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath()
		failOnErr(*mode, err)
		err = erasure.update(*file, *newFile)
		failOnErr(*mode, err)
	// case "recover":
	// 	//recover all the blocks of a disk and put the recovered result to new path
	// 	e.readConfig()
	// 	recover(*recoveredDiskPath)
	// case "scaling":
	// 	//scaling the system, ALERT: this is a system-level operation and irreversible
	// 	e.readConfig()
	// 	scaling(new_k, new_m)
	case "delete":
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath()
		failOnErr(*mode, err)
		err = erasure.removeFile(*file)
		failOnErr(*mode, err)
	default:
		log.Fatalf("Can't parse the parameters, please check %s!", *mode)
	}
	//It functions as a testbed, so currently I won't use goroutines.

}
