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
