//   @Author: your name
//   @Date: 2021-09-06 04:47:11
//   @LastEditTime: 2021-09-06 04:47:11
//   @LastEditors: Please set LastEditors
//   @Description: We decide to consider every operation in concurrent manner
//   @ProjectUrl: github.com/DurantVivado/Grasure

package Examples

import (
	"flag"
	"log"
	"time"
)

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

var erasure = main.Erasure{
	configFile:   "conf.json",
	fileMap:      make(map[string]*FileInfo),
	diskFilePath: ".hdr.disks.path",
	K:            k,
	M:            m,
	BlockSize:    blockSize,
	conStripes:   conStripes,
}

func main() {
	//We read each file and make byte flow
	flag_init()
	flag.Parse()
	//We read the config file
	start := time.Now()
	err = erasure.readDiskPath()
	failOnErr(mode, err)
	err = erasure.initSystem(false)
	failOnErr(mode, err)
	//We are entering the encoding mode, and for brevity,we only encode one filePath
	err = erasure.readConfig()
	failOnErr(mode, err)
	_, err := erasure.EncodeFile(filePath)
	failOnErr(mode, err)
	err = erasure.writeConfig()
	failOnErr(mode, err)
	err = erasure.updateConfigReplica()
	failOnErr(mode, err)

	//It functions as a testbed, so currently I won't use goroutines.
	log.Printf("%s consumes %.3f s", mode, time.Since(start).Seconds())
}
