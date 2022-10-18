package grasure

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	BASE = iota
	LOAD_BALANCE
	RANDOM
	GREEDY
)

// SGA is a fast-recovery  algorithm that balance the read block amount for each disk
// Currently, only support diskFail mode
//
// output: loadBalancedScheme stripeNum x K array
func (e *Erasure) getLoadBalancedDist(fi *fileInfo, gca_enable bool) (loadBalancedScheme [][]int, stripeOrder map[int][]int, err error) {
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
		return nil, nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNodeNum > e.M {
		return nil, nil, errTooFewDisksAlive
	}
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
	for k := range *failStripeSet {
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
		maxReduVec.Clear()
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
		fmt.Printf("---------------SGA Algorithm--------------")
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
		fmt.Printf("disk loads:\n%v\n", sumDisk)
	}
	// if !reflect.DeepEqual(diskLoads, sumDisk) {
	// 	return nil, fmt.Errorf("SGA verifying failed")
	// }
	// tmp := make([][]int, failStripeNum)
	// for i, s := range failStripeSet {
	// 	tmp[i] = loadBalancedScheme[s]
	// 	fmt.Printf("%v\n", loadBalancedScheme[s])
	// }
	if !gca_enable {
		return loadBalancedScheme, nil, nil
	}
	graph := make(map[int][]int)
	edgeNum := 0
	for s1 := range *failStripeSet {
		for s2 := range *failStripeSet {
			//if two nodes conflicts, then add an edge
			if s1 < s2 && isConflict(&loadBalancedScheme[s1], &loadBalancedScheme[s2], failNodeSet) {
				graph[s1] = append(graph[s1], s2)
				graph[s2] = append(graph[s2], s1)
				edgeNum++
			}
		}
	}
	if edgeNum == 0 {
		return loadBalancedScheme, nil, nil
	}
	//minTimeSlice is the minimal time slices needed for recovery
	minTimeSlice := 0
	//record records the used color
	record := IntSet{}
	//stripeColor marks the color of each stripe
	stripeColor := make(map[int]int)
	stripeOrder = make(map[int][]int)
	//we use the disk_vec generated from last step
	//to give assistance to coloring sequence
	cur, maxColor := 0, 0
	for s := range *failStripeSet {
		if _, ok := stripeColor[s]; !ok {
			cur = s
			maxColor = 0
			record.Clear()
			for _, neig := range graph[cur] {
				record.Insert(stripeColor[neig])
				maxColor = max(maxColor, stripeColor[neig])
			}
			for t := 1; t <= maxColor+1; t++ {
				if !record.Exist(t) {
					stripeColor[cur] = t
					stripeOrder[t] = append(stripeOrder[t], cur)
					minTimeSlice = max(minTimeSlice, t)
					break
				}
			}
		}
	}

	return loadBalancedScheme, stripeOrder, nil
}

// SGA algorithm
func (e *Erasure) SGA(filename string, options *Options) (
	map[string]string, error) {
	baseFileName := filepath.Base(filename)
	ReplaceMap := make(map[string]string)
	intFi, ok := e.fileMap.Load(baseFileName)
	if !ok {
		return nil, errFileNotFound
	}
	fi := intFi.(*fileInfo)

	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	//first we check the number of alive disks
	// to judge if any part need reconstruction
	alive := int32(0)
	failed := int32(0)
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)

	for i, disk := range e.diskInfos[:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				ReplaceMap[disk.diskPath] =
					e.diskInfos[e.DiskNum+int(failed)].diskPath
				atomic.AddInt32(&failed, 1)
				return &diskError{disk.diskPath,
					" available flag set false"}
			}
			ifs[i], err = os.Open(blobPath)
			if err != nil {
				disk.available = false
				ReplaceMap[disk.diskPath] =
					e.diskInfos[e.DiskNum+int(failed)].diskPath
				atomic.AddInt32(&failed, 1)
				return err
			}

			disk.available = true
			atomic.AddInt32(&alive, 1)
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("%s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < e.DiskNum; i++ {
			if ifs[i] != nil {
				ifs[i].Close()
			}
		}
	}()
	if int(alive) < e.K {
		//the disk renders unrecoverable
		return nil, errTooFewDisksAlive
	}
	//---------------------------------------
	//stripeOrder arranges the repairing order of each stripe
	//i.e., stripe[t] contains the failed stripe(s) that could be
	// recover in the t_th time slice. So on and so forth.
	var stripeOrder map[int][]int
	//---------------------------------------
	if int(alive) == e.DiskNum {
		if !e.Quiet {
			log.Println("start reading blocks")
		}
	} else {
		if !e.Quiet {
			log.Println("start reconstructing blocks")
		}

		start := time.Now()
		fi.loadBalancedScheme, stripeOrder, err =
			e.getLoadBalancedDist(fi, options.WithGCA)
		if err != nil {
			return nil, err
		}
		fmt.Printf("sga algorithm consumes:%q\n", time.Since(start))
	}

	//Since the file is striped, we have to reconstruct each stripe
	//for each stripe we rejoin the data
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	stripeCnt := 0
	nextStripe := 0
	//-----------------------------------
	//diskLoads records the load level of each disks(in blocks).
	diskLoads := make([]int32, e.DiskNum)
	//-----------------------------------

	//Without SGA: for every stripe pick up the first k alive blocks for repairing
	//With SGA: for maximal load-balance, pick up k alive blocks chosen by SGA
	//
	//Without GCA: the stripes are repaired in consecutive order
	//With GCA:  the stripes are repaired concurrently so that the total time slice is minimized
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			// offset := int64(subCnt) * e.allStripeSize
			func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//read all blocks in parallel
				//We only have to read k blocks to rec
				failList := make(map[int]bool)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[stripeNo][i]
					disk := e.diskInfos[diskId]
					blkStat := fi.blockInfos[stripeNo][i]
					if !disk.available || blkStat.bstat != blkOK {
						failList[diskId] = true
						continue
					}
					erg.Go(func() error {

						//we also need to know the block's accurate offset with respect to disk
						offset := fi.blockToOffset[stripeNo][i]
						_, err := ifs[diskId].ReadAt(
							blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
							int64(offset)*e.BlockSize)
						// fmt.Println("Read ", n, " bytes at", i, ", block ", block)
						if err != nil && err != io.EOF {
							return err
						}
						return nil
					})
				}
				if err := erg.Wait(); err != nil {
					return err
				}
				//Split the blob into k+m parts
				splitData, err := e.splitStripe(blobBuf[s])
				if err != nil {
					return err
				}
				//verify and reconstruct if broken
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				//the failed ones are left to next step
				if !ok && !options.WithGCA {

					if options.WithSGA {
						err = e.enc.ReconstructWithKBlocks(splitData,
							&failList,
							&fi.loadBalancedScheme[stripeNo],
							&(fi.Distribution[stripeNo]),
							options.Degrade)
					} else {
						err = e.enc.ReconstructWithList(splitData,
							&failList,
							&(fi.Distribution[stripeNo]),
							options.Degrade)
					}
					if err != nil {
						return err
					}
					tempCnt := 0
					for _, disk := range dist[stripeNo] {
						if _, ok := failList[disk]; !ok {
							atomic.AddInt32(&diskLoads[disk], 1)
							tempCnt++
							if tempCnt >= e.K {
								break
							}
						}
					}
				}
				return nil
			}()

		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}
	if options.WithGCA {
		//the reading upheld by GCA algorithm
		minTimeSlice := len(stripeOrder)
		for t := 1; t <= minTimeSlice; t++ {
			eg := e.errgroupPool.Get().(*errgroup.Group)
			strps := stripeOrder[t]
			blobBuf := makeArr2DByte(len(strps), int(e.allStripeSize))
			for s, stripeNo := range strps {
				stripeNo := stripeNo
				s := s
				eg.Go(func() error {
					erg := e.errgroupPool.Get().(*errgroup.Group)
					defer e.errgroupPool.Put(erg)
					//read all blocks in parallel
					//We only have to read k blocks to rec
					failList := make(map[int]bool)
					for i := 0; i < e.K+e.M; i++ {
						i := i
						diskId := dist[stripeNo][i]
						disk := e.diskInfos[diskId]
						blkStat := fi.blockInfos[stripeNo][i]
						if !disk.available || blkStat.bstat != blkOK {
							failList[diskId] = true
							continue
						}
						erg.Go(func() error {

							//we also need to know the block's accurate offset with respect to disk
							offset := fi.blockToOffset[stripeNo][i]
							_, err := ifs[diskId].ReadAt(
								blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
								int64(offset)*e.BlockSize)
							// fmt.Println("Read ", n, " bytes at", i, ", block ", block)
							if err != nil && err != io.EOF {
								return err
							}
							return nil
						})
					}
					if err := erg.Wait(); err != nil {
						return err
					}
					//Split the blob into k+m parts
					splitData, err := e.splitStripe(blobBuf[s])
					if err != nil {
						return err
					}

					err = e.enc.ReconstructWithKBlocks(splitData,
						&failList,
						&fi.loadBalancedScheme[stripeNo],
						&(fi.Distribution[stripeNo]),
						options.Degrade)
					if err != nil {
						return err
					}
					//----------------------------------------
					tempCnt := 0
					for _, disk := range fi.loadBalancedScheme[stripeNo] {
						if _, ok := failList[disk]; !ok {
							atomic.AddInt32(&diskLoads[disk], 1)
							tempCnt++
							if tempCnt >= e.K {
								break
							}
						}
					}

					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				return nil, err
			}
			e.errgroupPool.Put(eg)
		}
	}
	if !e.Quiet {
		//--------------------------------------------
		// fmt.Printf("------------------Normal--------------------")
		// maxload, sumload := 0, 0
		// for i := range diskLoads {
		// 	maxload = max(maxload, int(diskLoads[i]))
		// 	sumload += int(diskLoads[i])
		// }
		// fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxload, sumload)
		// fmt.Printf("disk loads:\n%v\n", diskLoads)
		//-------------------------------------------
		log.Printf("reading %s...", filename)
	}
	return ReplaceMap, nil
}
