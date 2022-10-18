// gca-p: gca algorithm with partial-stripe view
package grasure

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

// GCA-P algorithm
func (e *Erasure) GCA_P(filename string, options *Options) (
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
