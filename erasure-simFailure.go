package grasure

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
)

// the proportion of stripe where bitrot occurs of all stripes
const stripeFailProportion = 0.3

// Destroy simulates disk failure or bitrot:
//
// for `diskFail mode`, `failNum` random disks are marked as unavailable, `failName` is ignored.
//
// for `bitRot`, `failNum` random blocks in a stripe of the file corrupts, that only works in Read Mode;
//
// Since it's a simulation, no real data will be lost.
// Note that failNum = min(failNum, DiskNum).
func (e *Erasure) Destroy(simOption *SimOptions) {
	//if disk is currently unhealthy then give up
	if !e.isDiskHealthy() {
		return
	}
	//if failDisk is specialized, then use that
	if simOption.FailDisk != "" {
		disks := strings.Split(simOption.FailDisk, ",")
		//we randomly picked up failNum disks and mark as unavailable
		if !e.Quiet {
			log.Println("simulate failure on:")
		}
		for _, d := range disks {
			id, _ := strconv.Atoi(d)
			e.diskInfos[id].available = false
			fmt.Printf("%s(%d)\n", e.diskInfos[id].diskPath, id)
		}
		return
	}
	if simOption.Mode == "diskFail" || simOption.Mode == "DiskFail" {
		if simOption.FailNum <= 0 {
			return
		}
		if simOption.FailNum > e.DiskNum {
			simOption.FailNum = e.DiskNum
		}

		//we randomly picked up failNum disks and mark as unavailable
		if !e.Quiet {
			log.Println("simulate failure on:")
		}

		shuff := genRandomArr(e.DiskNum, 0)
		for i := 0; i < simOption.FailNum; i++ {

			if !e.Quiet {
				fmt.Printf("%s(%d)\n", e.diskInfos[shuff[i]].diskPath, i)
			}
			e.diskInfos[shuff[i]].available = false
		}
	} else if simOption.Mode == "bitRot" || simOption.Mode == "BitRot" {
		//in thi smode, we don't really corrupt a bit. Instead, we mark the block containing rots as failed
		// which is omnipresent is today's storage facilities.
		//if fileName is "", we corrupt all the files, else corrupt specific file
		if simOption.FailNum > e.K+e.M {
			simOption.FailNum = e.K + e.M
		}
		if simOption.FileName == "" {
			e.fileMap.Range(func(filename, fi interface{}) bool {
				fd := fi.(*fileInfo)
				//of course the bitrot must be random, and pesudo-random
				//algorithms have flaws. For every stripe, we corrupt failNum blocks
				stripeNum := len(fd.blockInfos)
				stripeFail := int(stripeFailProportion * float32(stripeNum))
				for i := range genRandomArr(stripeNum, 0)[:stripeFail] {

					for j := range genRandomArr(e.K+e.M, 0)[:simOption.FailNum] {
						fd.blockInfos[i][j].bstat = blkFail
					}
				}
				return true
			})
		} else {
			baseFileName := filepath.Base(simOption.FileName)
			intFi, ok := e.fileMap.Load(baseFileName)
			if !ok {
				log.Fatal(errFileNotFound)
			}
			fi := intFi.(*fileInfo)

			//of course the bitrot must be random, and pesudo-random
			//algorithms have flaws. For every stripe, we corrupt simOption.FailNum blocks
			stripeNum := len(fi.blockInfos)
			stripeFail := int(stripeFailProportion * float32(stripeNum))
			strps := genRandomArr(stripeNum, 0)[:stripeFail]
			for _, i := range strps {

				blks := genRandomArr(e.K+e.M, 0)[:simOption.FailNum]
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

// print disk status
func (e *Erasure) printDiskStatus() {
	for i, disk := range e.diskInfos {

		fmt.Printf("DiskId:%d, available:%tn,numBlocks:%d, storage:%d/%d (bytes)\n",
			i, disk.available, disk.numBlocks, int64(disk.numBlocks)*e.BlockSize, disk.capacity)
	}
}

// check system health
// 1. if currently working disks' number is less than DiskNum, inform the user
func (e *Erasure) isDiskHealthy() bool {
	for _, v := range e.diskInfos[:e.DiskNum] {
		if !v.available {
			return false
		}
	}
	return true
}
