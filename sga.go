package grasure

import (
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

const (
	BASE = iota
	LOAD_BALANCE
	RANDOM
	GREEDY
)

type SGA struct {
	*Erasure
	failNodeNum       int
	failStripeNum     int
	maxRepairPerSlice int
	minTimeSlice      int
	minMaxLoad        int
	//each failed node corresponds to a backup node
	graph          map[int][]int
	stripeToRepair []int
	stripeColor    []int
	matchStripeSet [][]int
	diskloadDict   [][]int
}

func (e *Erasure) SGA(fi *fileInfo) (loadBalancedScheme [][]int, err error) {

	baseFileName := fi.FileName
	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	erg := new(errgroup.Group)
	//summarize the number of failed disks
	failNum := int32(0)
	failNodeSet := make(map[int]int)
	for _, disk := range e.diskInfos[:e.DiskNum] {
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				atomic.AddInt32(&failNum, 1)
				return &diskError{disk.diskPath, " available flag set false"}
			}
			_, err = os.Open(blobPath)
			if err != nil {
				disk.available = false
				atomic.AddInt32(&failNum, 1)
				return err
			}

			disk.available = true
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("%s", err.Error())
		}
	}
	if failNum == 0 {
		//no need for recovering
		return dist, nil
	}
	if int(failNum) > e.M {
		return nil, errTooFewDisksAlive
	}

	maxRepairPerSlice := (e.DiskNum - int(failNum)) / e.M
	if !e.Quiet {
		log.Println("The maximum stripes could be repaired in parallel is", sga.maxRepairPerSlice)
	}
	failStripeNum := 0
	for i := 0; i < stripeNum; i++ {
		for j := 0; j < e.K+e.M; j++ {
			if _, ok := failNodeSet[dist[i][j]]; ok {
				failStripeNum++
				break
			}
		}
	}
	minTimeSlice := failStripeNum
	loadBalancedScheme = makeArr2DInt(stripeNum, e.K+e.M)

	return loadBalancedScheme, nil
}
