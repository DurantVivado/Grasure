//   @Author: your name
//   @Date: 2021-09-06 04:47:11
//   @LastEditTime: 2021-09-06 04:47:11
//   @LastEditors: Please set LastEditors
//   @Description: We decide to consider every operation in concurrent manner
//   @ProjectUrl: github.com/DurantVivado/Grasure

package examples

import (
	"flag"
	"log"
	"time"
)

type Erasure Grasure.Erasure

var (
	k          int
	m          int
	blockSize  int64
	conStripes int
	err        error
	mode       string
	filePath   string
)

func flag_init() {

}

var failOnErr = func(mode string, e error) {
	if e != nil {
		log.Fatalf("%s: %s", mode, e.Error())
	}
}

func main() {
	//We read each file and make byte flow
	flag_init()
	flag.Parse()
	erasure := Erasure{
		configFile:   "conf.json",
		diskFilePath: ".hdr.disks.path",
		K:            k,
		M:            m,
		BlockSize:    blockSize,
		conStripes:   conStripes,
	}
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
