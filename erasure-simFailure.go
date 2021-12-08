package grasure

import (
	"fmt"
	"log"
)

//Destroy simulates disk failure or bitrot:
//
//for `diskFail mode`, `failNum` random disks are marked as unavailable, `failName` is ignored.
//
// for `bitRot`, `failNum` random blocks in a stripe of the file corrupts;
//
// Since it's a simulation, no real data will be lost.
// Note that failNum = min(failNum, DiskNum).
func (e *Erasure) Destroy(mode string, failNum int, fileName string) {
	if mode == "diskFail" {
		if failNum <= 0 {
			return
		}
		if failNum > e.DiskNum {
			failNum = e.DiskNum
		}
		//if disk is currently unhealthy then give up
		if !e.isDiskHealthy() {
			return
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
		//in thismode, not a full blob is missing. Instead, several blocks
		// corrupts known as bit rot, which is omnipresent is today's storage facilities.
		//if fileName is "", we corrupt all the files, else corrupt specific file
		if fileName != "" {

		} else {

		}
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
