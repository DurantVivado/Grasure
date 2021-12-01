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
)

func main() {

	flag_init()
	flag.Parse()
	erasure := &Erasure{
		configFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		diskFilePath: ".hdr.disks.path",
		DiskNum:      diskNum,
		K:            k,
		M:            m,
		BlockSize:    blockSize,
		conStripes:   conStripes,
		override:     override,
		quiet:        quiet,
	}
	//We read the config file
	// ctx, _ := context.WithCancel(context.Background())
	// go monitorCancel(cancel)
	start := time.Now()
	err = erasure.readDiskPath()
	failOnErr(mode, err)

	switch mode {
	case "init":
		err = erasure.initSystem(false)
		failOnErr(mode, err)
	case "read":
		//read a file
		err = erasure.readConfig()
		failOnErr(mode, err)
		erasure.destroy(failMode, failNum)
		err = erasure.readFile(filePath, savePath)
		failOnErr(mode, err)

	case "encode":
		//We are entering the encoding mode, and for brevity,we only encode one filePath
		err = erasure.readConfig()
		failOnErr(mode, err)
		_, err := erasure.EncodeFile(filePath)
		failOnErr(mode, err)
		err = erasure.writeConfig()
		failOnErr(mode, err)
		err = erasure.updateConfigReplica()
		failOnErr(mode, err)
	case "update":
		//update an old file according to a new file
		err = erasure.readConfig()
		failOnErr(mode, err)
		err = erasure.update(filePath, newFilePath)
		failOnErr(mode, err)
		err = erasure.writeConfig()
		failOnErr(mode, err)
		err = erasure.updateConfigReplica()
		failOnErr(mode, err)
	// case "recover":
	// 	//recover all the blocks of a disk and put the recovered result to new path
	// 	e.readConfig()
	// 	recover(recoveredDiskPath)
	// case "scaling":
	// 	//scaling the system, ALERT: this is a system-level operation and irreversible
	// 	e.readConfig()
	// 	scaling(new_k, new_m)
	case "delete":

		err = erasure.removeFile(filePath)
		failOnErr(mode, err)
		err = erasure.writeConfig()
		failOnErr(mode, err)
		err = erasure.updateConfigReplica()
		failOnErr(mode, err)
	default:
		log.Fatalf("Can't parse the parameters, please check %s!", mode)
	}
	//It functions as a testbed, so currently I won't use goroutines.
	log.Printf("%s consumes %.3f s", mode, time.Since(start).Seconds())
}
