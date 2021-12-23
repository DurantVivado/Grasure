package grasure

import (
	"log"
	"sort"
	"sync"
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

func (e *Erasure) SGA() (loadBalancedScheme map[string][][]int, err error) {
	totalFiles := e.getFileNum()
	if !e.Quiet {
		log.Printf("Start recovering, totally %d files need recovery",
			totalFiles)
	} //first, make clear how many disks need to be recovered
	//Second, match backup partners
	//Third, concurrently recover the part of the files
	failNum := 0
	j := e.DiskNum
	failNodeSet := make(map[int]int)
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			failNodeSet[i] = j
			j++
			if j >= len(e.diskInfos) {
				return nil, errNotEnoughBackupForRecovery
			}
			failNum++
		}
	}
	if failNum == 0 {
		return nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNum > e.M {
		return nil, errTooFewDisksAlive
	}
	erg := new(errgroup.Group)
	maxRepairPerSlice := (e.DiskNum - int(failNum)) / e.M
	if !e.Quiet {
		log.Println("The maximum stripes could be repaired in parallel is", sga.maxRepairPerSlice)
	}
	//failStripeNum is the number of failed stripes
	failStripeNum := int32(0)
	//failStripeSet marks which file has which failed stripe
	failStripeSet := make(map[string][]int, totalFiles)
	//diskLoads marks the number of blocks for each disk
	diskLoads := make([]int, e.DiskNum)
	//sum up total failed stripe number
	var mu sync.Mutex
	e.fileMap.Range(func(filename, fd interface{}) bool {
		erg.Go(func() error {
			basefilename := filename.(string)
			fi := fd.(*fileInfo)
			fileSize := fi.FileSize
			stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
			dist := fi.Distribution
			mu.Lock()
			loadBalancedScheme[basefilename] = makeArr2DInt(stripeNum, e.K)
			mu.Unlock()
			for i := 0; i < stripeNum; i++ {
				flag := true
				for j := 0; j < e.K+e.M; j++ {
					if _, ok := failNodeSet[dist[i][j]]; ok {
						e.mu.Lock()
						diskLoads[dist[i][j]]++
						e.mu.Unlock()
						flag = false
					}

				}
				if !flag {
					atomic.AddInt32(&failStripeNum, 1)
					mu.Lock()
					failStripeSet[basefilename] = append(failStripeSet[basefilename], i)
					mu.Unlock()
				}
			}
			return nil
		})
		return true
	})
	if err := erg.Wait(); err != nil {

	}
	minTimeSlice := failStripeNum
	//avlbleSum is the total redundant blocks
	avlbleSum := e.M * int(failStripeNum)
	// maxload_idx is the current maximal reducible load's index
	maxload_idx := e.DiskNum - 1
	for avlbleSum > 0 {
		//we obtain current load set for each disk and sort in descending order
		tempDiskLoad := diskLoads
		tempDiskLoad = sort.IntSlice(tempDiskLoad)
		curMaxLoad = tempDiskLoad[maxload_idx]

	}
	return loadBalancedScheme, nil
}
