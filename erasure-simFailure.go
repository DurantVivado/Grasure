package grasure

import (
	"fmt"
	"log"
	"path/filepath"
)

//the proportion of stripe where bitrot occurs of all stripes
const stripeFailProportion = 0.3

//Destroy simulates disk failure or bitrot:
//
//for `diskFail mode`, `failNum` random disks are marked as unavailable, `failName` is ignored.
//
// for `bitRot`, `failNum` random blocks in a stripe of the file corrupts, that only works in Read Mode;
//
// Since it's a simulation, no real data will be lost.
// Note that failNum = min(failNum, DiskNum).
func (e *Erasure) Destroy(mode string, failNum int, fileName string) {
	//if disk is currently unhealthy then give up
	if !e.isDiskHealthy() {
		return
	}
	if mode == "diskFail" {
		if failNum <= 0 {
			return
		}
		if failNum > e.DiskNum {
			failNum = e.DiskNum
		}

		//we randomly picked up failNum disks and mark as unavailable
		if !e.Quiet {
			log.Println("simulate failure on:")
		}

		shuff := genRandomArr(e.DiskNum, 0)
		for i := 0; i < failNum; i++ {

			if !e.Quiet {
				log.Println(e.diskInfos[shuff[i]].diskPath)
			}
			e.diskInfos[shuff[i]].available = false
		}
	} else if mode == "bitRot" {
		//in thi smode, we don't really corrupt a bit. Instead, we mark the block containing rots as failed
		// which is omnipresent is today's storage facilities.
		//if fileName is "", we corrupt all the files, else corrupt specific file
		if failNum > e.K+e.M {
			failNum = e.K + e.M
		}
		if fileName == "" {
			e.fileMap.Range(func(filename, fi interface{}) bool {
				fd := fi.(*fileInfo)
				//of course the bitrot must be random, and pesudo-random
				//algorithms have flaws. For every stripe, we corrupt failNum blocks
				stripeNum := len(fd.blockInfos)
				stripeFail := int(stripeFailProportion * float32(stripeNum))
				for i := range genRandomArr(stripeNum, 0)[:stripeFail] {

					for j := range genRandomArr(e.K+e.M, 0)[:failNum] {
						fd.blockInfos[i][j].bstat = blkFail
					}
				}
				return true
			})
		} else {
			baseFileName := filepath.Base(fileName)
			intFi, ok := e.fileMap.Load(baseFileName)
			if !ok {
				log.Fatal(errFileNotFound)
			}
			fi := intFi.(*fileInfo)

			//of course the bitrot must be random, and pesudo-random
			//algorithms have flaws. For every stripe, we corrupt failNum blocks
			stripeNum := len(fi.blockInfos)
			stripeFail := int(stripeFailProportion * float32(stripeNum))
			strps := genRandomArr(stripeNum, 0)[:stripeFail]
			for _, i := range strps {

				blks := genRandomArr(e.K+e.M, 0)[:failNum]
				for _, j := range blks {
					// fmt.Printf("i:%d, j :%d fails.\n", i, j)
					fi.blockInfos[i][j].bstat = blkFail
				}
			}

		}
	} else {
		log.Fatal("please specialize failMode in diskFail and bitRot")
	}
}

//print disk status
func (e *Erasure) printDiskStatus() {
	for i, disk := range e.diskInfos {

		fmt.Printf("DiskId:%d, available:%tn,numBlocks:%d, storage:%d/%d (bytes)\n",
			i, disk.available, disk.numBlocks, int64(disk.numBlocks)*e.BlockSize, disk.capacity)
	}
}

//check system health
// 1. if currently working disks' number is less than DiskNum, inform the user
func (e *Erasure) isDiskHealthy() bool {
	for _, v := range e.diskInfos[:e.DiskNum] {
		if !v.available {
			return false
		}
	}
	return true
}
