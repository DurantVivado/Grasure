// gca-f: gca algorithm with full-stripe preview
package grasure

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) getParalledDist(fi *fileInfo) (
	loadBalancedScheme [][]int, stripeOrder map[int][]int, err error) {
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
	// diskLoads := make([]int, e.DiskNum)
	//diskLoads records every disk's stripe information
	// diskDict := make([]IntSet, e.DiskNum)
	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	loadBalancedScheme = make([][]int, stripeNum)

	//maxLoad calculates the maximal load of disks
	// maxLoad := 0
	//sum is the loads of each disk
	// sumDisk := make([]int, e.DiskNum)
	//sumLoad is the total load of all disks
	// sumLoad := 0
	for i := 0; i < stripeNum; i++ {
		flag := true
		failBlk := 0
		for j := 0; j < e.K+e.M; j++ {
			if failNodeSet.Exist(dist[i][j]) {
				failBlk++
				flag = false
				break
			}
		}
		if !flag {
			failStripeNum += 1
			failStripeSet.Insert(i)

		}
	}
	//for every broken stripe, randomly pick up k blocks for recovery
	for s := 0; s < stripeNum; s++ {
		if failStripeSet.Exist(s) {
			disk_vec := make([]int, 0)
			for i := 0; i < e.K+e.M; i++ {
				if !failNodeSet.Exist(dist[s][i]) {
					disk_vec = append(disk_vec, dist[s][i])
				}
			}
			// fmt.Println("before shuffle:", disk_vec)
			rand.Shuffle(len(disk_vec), func(i, j int) {
				disk_vec[i], disk_vec[j] = disk_vec[j], disk_vec[i]
			})
			// fmt.Println("after shuffle:", disk_vec)
			// for j := 0; j < e.K; j++ {
			// 	sumDisk[dist[s][disk_vec[j]]]++
			// 	maxLoad = max(maxLoad, sumDisk[dist[s][disk_vec[j]]])
			// 	sumLoad++
			// }
			// fmt.Println("sampled:", disk_vec[:e.K])
			loadBalancedScheme[s] = append(loadBalancedScheme[s], disk_vec[:e.K]...)

		} else {
			loadBalancedScheme[s] = dist[s]
		}
	}
	// if !e.Quiet {
	// fmt.Printf("---------------GCA Algorithm--------------")
	// fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxLoad, sumLoad)
	// fmt.Printf("disk loads:\n%v\n", sumDisk)
	// }
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
	maxStripe := e.MemSize * GiB / e.dataStripeSize
	fmt.Println("max stripe number:", maxStripe)
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
					// consider the memory limit
					if len(stripeOrder[t]) < int(maxStripe) {
						stripeOrder[t] = append(stripeOrder[t], cur)
						minTimeSlice = max(minTimeSlice, t)
						break
					}
					fmt.Println("skip")
				}
			}
		}
	}

	return loadBalancedScheme, stripeOrder, nil
}

// SGA algorithm
func (e *Erasure) GCA_F(filename string, options *Options) (
	map[string]string, error) {
	baseFileName := filepath.Base(filename)
	ReplaceMap := make(map[string]string)
	intFi, ok := e.fileMap.Load(baseFileName)
	if !ok {
		return nil, errFileNotFound
	}
	fi := intFi.(*fileInfo)

	// fileSize := fi.FileSize
	// stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
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
			e.getParalledDist(fi)
		if err != nil {
			return nil, err
		}
		fmt.Printf("gca algorithm consumes:%q\n", time.Since(start))
	}

	//Since the file is striped, we have to reconstruct each stripe
	//for each stripe we rejoin the data
	//-----------------------------------
	//diskLoads records the load level of each disks(in blocks).
	// diskLoads := make([]int32, e.DiskNum)
	//-----------------------------------

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
				// fmt.Printf("stripeNo:%d, %v\n", stripeNo, fi.loadBalancedScheme[stripeNo])
				err = e.enc.ReconstructWithKBlocks(splitData,
					&failList,
					&fi.loadBalancedScheme[stripeNo],
					&(fi.Distribution[stripeNo]),
					options.Degrade)
				if err != nil {
					return err
				}
				//----------------------------------------
				// tempCnt := 0
				// for _, disk := range fi.loadBalancedScheme[stripeNo] {
				// 	if _, ok := failList[disk]; !ok {
				// 		atomic.AddInt32(&diskLoads[disk], 1)
				// 		tempCnt++
				// 		if tempCnt >= e.K {
				// 			break
				// 		}
				// 	}
				// }

				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
	}

	if !e.Quiet {
		log.Printf("reading %s...", filename)
	}
	return ReplaceMap, nil
}
