package grasure

import (
	"fmt"
	"sort"
)

const (
	BASE = iota
	LOAD_BALANCE
	RANDOM
	GREEDY
)

//SGA is a fast-recovery  algorithm that balance the read block amount for each disk
//Currently, only support diskFail mode
//
//input : fileInfo
//
//output: loadBalancedScheme stripeNum x K array
func (e *Erasure) SGA(fi *fileInfo) (loadBalancedScheme [][]int, err error) {
	//first, make clear how many disks need to be recovered
	//Second, match backup partners
	//Third, concurrently recover the part of the files
	failNodeNum := 0
	failNodeSet := &IntSet{}
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			failNodeSet.Insert(i)
			failNodeNum++
		}
	}
	if failNodeNum == 0 {
		return nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNodeNum > e.M {
		return nil, errTooFewDisksAlive
	}
	// maxRepairPerSlice := (e.DiskNum - int(failNodeNum)) / e.M
	// if !e.Quiet {
	// 	log.Println("The maximum stripes could be repaired in parallel is", sga.maxRepairPerSlice)
	// }
	//failStripeNum is the number of failed stripes
	failStripeNum := 0
	//failStripeSet marks which file has which failed stripe
	failStripeSet := &IntSet{}
	//diskLoads marks the number of blocks for each disk
	diskLoads := make([]int, e.DiskNum)
	//diskLoads records every disk's stripe information
	diskDict := make([]IntSet, e.DiskNum)
	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	loadBalancedScheme = make([][]int, stripeNum)
	//stripeRedu records how many blocks a failed stripe can reduce
	stripeRedu := make(map[int]int)
	//avlbleSum is the total redundant blocks
	avlbleSum := 0
	for i := 0; i < stripeNum; i++ {
		flag := true
		failBlk := 0
		for j := 0; j < e.K+e.M; j++ {
			if failNodeSet.Exist(dist[i][j]) {

				if _, ok := stripeRedu[i]; ok {
					stripeRedu[i]--
				} else {
					stripeRedu[i] = e.M - 1
				}
				failBlk++
				flag = false
			} else {
				diskLoads[dist[i][j]]++
				diskDict[dist[i][j]].Insert(i)
			}

		}
		if failBlk > 0 {
			avlbleSum += (e.M - failBlk)
		}
		if !flag {
			failStripeNum += 1
			failStripeSet.Insert(i)

		}
	}
	failStripeVec := []int{}
	for k, _ := range *failStripeSet {
		failStripeVec = append(failStripeVec, k)
	}
	sort.Ints(failStripeVec)

	// maxload_idx is the current maximal reducible load's index
	maxload_idx := e.DiskNum - 1
	//failReduList records the disks failing to reduce in maxReduVec
	failReduList := &IntSet{}
	//maxReduVec records the maximal loaded disk set w.r.t stripe
	maxReduVec := &IntSet{}

	last_avlbleSum := 0
	for avlbleSum > 0 {
		//we obtain current load set for each disk and sort in descending order
		tempDiskLoad := make([]int, e.DiskNum)
		copy(tempDiskLoad, diskLoads)
		sort.Ints(tempDiskLoad)
		curMaxLoad := tempDiskLoad[maxload_idx]
		maxRedu := 0
		for i := 0; i < e.DiskNum; i++ {
			if !failNodeSet.Exist(i) && !failReduList.Exist(i) && diskLoads[i] == curMaxLoad {
				reduNum := len(diskDict[i])
				if reduNum > maxRedu {
					maxReduVec.Clear()
					maxReduVec.Insert(i)
					maxRedu = reduNum
				} else if reduNum == maxRedu {
					maxReduVec.Insert(i)
				}
			}

		}
		if maxReduVec.Empty() {
			maxload_idx--
			continue
		}
		//if current maximally loaded disk are fully reduced
		//we don't have to judge whether the current maxmimal load is accessible
		isMaxReducible := false
		for j := range *maxReduVec {
			for k := range diskDict[j] {
				if stripeRedu[k] > 0 {
					avlbleSum--
					stripeRedu[k]--
					diskDict[j].Erase(k)
					diskLoads[j]--
					isMaxReducible = true
					break
				}
				if isMaxReducible {
					break
				}
			}
		}
		//if current maximally loaded disk are fully reduced
		//He could borrow some money from previously the richest relatives, for illustration
		if !isMaxReducible {
			for j := range *maxReduVec {
				for s := range *failStripeSet {
					for n := 0; n < e.DiskNum; n++ {
						if !failReduList.Exist(n) &&
							!failNodeSet.Exist(n) &&
							!diskDict[j].Exist(s) &&
							diskDict[n].Exist(s) &&
							diskLoads[n] == diskLoads[j]-1 {

							diskDict[n].Insert(s)
							diskDict[j].Erase(s)
							diskLoads[n]++
							diskLoads[j]--
							isMaxReducible = true
							break
						}

					}
					if isMaxReducible {
						break
					}
				}
				if avlbleSum == last_avlbleSum {
					failReduList.Insert(j)
				}
				if isMaxReducible {
					break
				}
			}
		}
		last_avlbleSum = avlbleSum
	}
	//maxLoad calculates the maximal load of disks
	maxLoad := 0
	//sum is the loads of each disk
	sumDisk := make([]int, e.DiskNum)
	//sumLoad is the total load of all disks
	sumLoad := 0
	//mapInd stores disk-to-block_index mapping
	// mapInd := make([]int, e.DiskNum)
	//for every stripe, determine which stripes chosen for recovery
	for s := 0; s < stripeNum; s++ {
		if failStripeSet.Exist(s) {
			for i := 0; i < e.K+e.M; i++ {
				if !failNodeSet.Exist(dist[s][i]) && diskDict[dist[s][i]].Exist(s) {
					loadBalancedScheme[s] = append(loadBalancedScheme[s], dist[s][i])
					sumDisk[dist[s][i]]++
					maxLoad = max(maxLoad, sumDisk[dist[s][i]])
					sumLoad++
				}
			}
		} else {
			loadBalancedScheme[s] = dist[s]
		}
	}
	if !e.Quiet {
		fmt.Printf("----------SGA Algorithm----------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
		fmt.Printf("---------------------------------")
	}
	// if !reflect.DeepEqual(diskLoads, sumDisk) {
	// 	return nil, fmt.Errorf("SGA verifying failed")
	// }
	// tmp := make([][]int, failStripeNum)
	// for i, s := range failStripeSet {
	// 	tmp[i] = loadBalancedScheme[s]
	// 	fmt.Printf("%v\n", loadBalancedScheme[s])
	// }
	return loadBalancedScheme, nil
}
