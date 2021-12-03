//   @Author: your name
//   @Date: 2021-09-06 04:47:11
//   @LastEditTime: 2021-09-06 04:47:11
//   @LastEditors: Please set LastEditors
//   @Description: We decide to consider every operation in concurrent manner
//   @ProjectUrl: github.com/DurantVivado/Grasure

package main

import (
	"flag"
	"log"
	"time"

	grasure "github.com/DurantVivado/Grasure"
)

var failOnErr = func(mode string, e error) {
	if e != nil {
		log.Fatalf("%s: %s", mode, e.Error())
	}
}
var err error

func main() {

	flag_init()
	flag.Parse()
	erasure := &grasure.Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    ".hdr.disks.path",
		DiskNum:         diskNum,
		K:               k,
		M:               m,
		BlockSize:       blockSize,
		ConStripes:      conStripes,
		Override:        override,
		Quiet:           quiet,
		ReplicateFactor: replicateFactor,
	}
	//We read the config file
	// ctx, _ := context.WithCancel(context.Background())
	// go monitorCancel(cancel)
	start := time.Now()
	err = erasure.ReadDiskPath()
	failOnErr(mode, err)

	switch mode {
	case "init":
		err = erasure.InitSystem(false)
		failOnErr(mode, err)
	case "read":
		//read a file
		err = erasure.ReadConfig()
		failOnErr(mode, err)
		erasure.Destroy(failMode, failNum)
		err = erasure.ReadFile(filePath, savePath)
		failOnErr(mode, err)

	case "encode":
		//We are entering the encoding mode, and for brevity,we only encode one filePath
		err = erasure.ReadConfig()
		failOnErr(mode, err)
		_, err := erasure.EncodeFile(filePath)
		failOnErr(mode, err)
		err = erasure.WriteConfig()
		failOnErr(mode, err)
	case "update":
		//update an old file according to a new file
		// err = erasure.ReadConfig()
		// failOnErr(mode, err)
		// err = erasure.Update(filePath, newFilePath)
		// failOnErr(mode, err)
		// err = erasure.WriteConfig()
		// failOnErr(mode, err)
	case "recover":
		//recover all the blocks of a disk and put the recovered result to new path
		err = erasure.ReadConfig()
		failOnErr(mode, err)
		erasure.Destroy(failMode, failNum)
		err = erasure.Recover()
		failOnErr(mode, err)

	// case "scaling":
	// 	//scaling the system, ALERT: this is a system-level operation and irreversible
	// 	e.ReadConfig()
	// 	scaling(new_k, new_m)
	case "delete":

		err = erasure.RemoveFile(filePath)
		failOnErr(mode, err)
		err = erasure.WriteConfig()
		failOnErr(mode, err)
	default:
		log.Fatalf("Can't parse the parameters, please check %s!", mode)
	}
	//It functions as a testbed, so currently I won't use goroutines.
	log.Printf("%s consumes %.3f s", mode, time.Since(start).Seconds())
}

var (
	blockSize         int64
	mode              string
	k                 int
	m                 int
	diskNum           int
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
	quiet             bool
)

//the parameter lists, with fullname or abbreviation
func flag_init() {

	flag.StringVar(&mode, "md", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")
	flag.StringVar(&mode, "mode", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")

	flag.IntVar(&k, "k", 12, "the number of data shards(<256)")
	flag.IntVar(&k, "dataNum", 12, "the number of data shards(<256)")

	flag.IntVar(&m, "m", 4, "the number of parity shards(2-4)")
	flag.IntVar(&m, "parityNum", 4, "the number of parity shards(2-4)")

	flag.Int64Var(&blockSize, "bs", 4096, "the block size in bytes")
	flag.Int64Var(&blockSize, "blockSize", 4096, "the block size in bytes")

	flag.IntVar(&conStripes, "cs", 100, "how many stripes are allowed to encode/decode concurrently")
	flag.IntVar(&conStripes, "conStripes", 100, "how many stripes are allowed to encode/decode concurrently")

	flag.IntVar(&diskNum, "dn", 4, "the number of disks used in .hdr.disk.path")
	flag.IntVar(&diskNum, "diskNum", 4, "the number of disks used in .hdr.disk.path")

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

	flag.BoolVar(&override, "o", false, "whether or not to override former files or directories, default to false")
	flag.BoolVar(&override, "override", false, "whether or not to override former files or directories, default to false")

	flag.BoolVar(&conWrites, "cw", true, "whether or not to enable concurrent write, default is false")
	flag.BoolVar(&conWrites, "conWrites", true, "whether or not to enable concurrent write, default is false")

	flag.BoolVar(&conReads, "cr", true, "whether or not to enable concurrent read, default is false")
	flag.BoolVar(&conReads, "conReads", true, "whether or not to enable concurrent read, default is false")

	flag.StringVar(&failMode, "fmd", "diskFail", "simulate [diskFail] or [bitRot] mode")
	flag.StringVar(&failMode, "failMode", "diskFail", "simulate [diskFail] or [bitRot] mode")

	flag.IntVar(&failNum, "fn", 0, "simulate multiple disk failure, provides the fail number of disks")
	flag.IntVar(&failNum, "failNum", 0, "simulate multiple disk failure, provides the fail number of disks")

	flag.IntVar(&replicateFactor, "rf", 3, "the meta data is replicated `rf`- fold to provide enough reliability, default is 3-fold")
	flag.IntVar(&replicateFactor, "replicateFactor", 3, "the meta data is replicated `rf`- fold to provide enough reliability, default is 3-fold")

	flag.BoolVar(&quiet, "q", false, "if true mute outputs otherwise print them")
	flag.BoolVar(&quiet, "quiet", false, "if true mute outputs otherwise print them")

}
