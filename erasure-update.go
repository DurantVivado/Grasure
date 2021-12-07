package grasure

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

//update a file according to a new file, the local `filename` will be used to update the file in the cloud with the same name
func (e *Erasure) Update(oldFile, newFile string) error {
	// read old file info
	baseName := filepath.Base(oldFile)
	intFi, ok := e.fileMap.Load(baseName)
	if !ok {
		return errFileNotFound
	}
	fi := intFi.(*FileInfo)
	// update file info
	nf, err := os.Open(newFile)
	if err != nil {
		return err
	}
	defer nf.Close()
	fileInfo, err := nf.Stat()
	oldFileSize := fi.FileSize
	fi.FileSize = fileInfo.Size()
	hashStr, err := hashStr(nf)
	if err != nil {
		return err
	}
	fi.Hash = hashStr

	// open file as io.Reader
	alive := int32(0)
	diskNum := len(e.diskInfos)
	ifs := make([]*os.File, diskNum)
	erg := new(errgroup.Group)
	diskFail := false
	for i, disk := range e.diskInfos[:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				diskFail = true
				return &DiskError{disk.diskPath, " avilable flag set flase"}
			}
			ifs[i], err = os.OpenFile(blobPath, os.O_RDWR|os.O_TRUNC, 0666)
			if err != nil {
				disk.available = false
				return err
			}

			disk.available = true
			atomic.AddInt32(&alive, 1)
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("read failed %s", err.Error())
		}
		if diskFail {
			return err
		}
	}
	defer func() {
		for i := range e.diskInfos {
			ifs[i].Close()
		}
	}()
	if int(alive) < e.K {
		//the disk renders inrecoverable
		return errTooFewDisksAlive
	}
	if int(alive) == e.DiskNum {
		if !e.Quiet {
			log.Println("start reading blocks")
		}
	} else {
		if !e.Quiet {
			log.Println("start reconstructing blocks")
		}
	}

	oldStripeNum := int(ceilFracInt64(oldFileSize, e.dataStripeSize))
	newStripeNum := int(ceilFracInt64(fi.FileSize, e.dataStripeSize))
	// fmt.Println(oldStripeNum, newStripeNum)
	numBlob := ceilFracInt(newStripeNum, e.ConStripes)
	adjustDist(e, fi, oldStripeNum, newStripeNum)

	stripeCnt := 0
	nextStripe := 0
	dist := fi.Distribution
	newBlobBuf := makeArr2DByte(e.ConStripes, int(e.dataStripeSize))
	oldBlobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > newStripeNum {
			nextStripe = newStripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		// newBlobBuf := *e.dataBlobPool.Get().(*[][]byte)
		// oldBlobBuf := *e.allBlobPool.Get().(*[][]byte)
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			if stripeNo < oldStripeNum {
				// read old data shards
				// fmt.Println("old")
				eg.Go(func() error {
					erg := e.errgroupPool.Get().(*errgroup.Group)
					defer e.errgroupPool.Put(erg)
					for i := 0; i < e.K+e.M; i++ {
						i := i
						erg.Go(func() error {
							diskID := dist[stripeNo][i]
							disk := e.diskInfos[diskID]
							if !disk.available {
								return nil
							}
							offset := fi.blockToOffset[stripeNo][i]
							_, err := ifs[diskID].ReadAt(oldBlobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
								int64(offset)*e.BlockSize)
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
					oldData, err := e.SplitStripe(oldBlobBuf[s])
					if err != nil {
						return err
					}
					//verify and reconstruct
					ok, err := e.enc.Verify(oldData)
					if err != nil {
						return err
					}
					if !ok {
						// fmt.Println("reconstruct data of stripe:", stripeNo)
						err = e.enc.Reconstruct(oldData)
						if err != nil {
							return err
						}
					}
					// read new data shards
					offset := int64(stripeCnt+s) * e.dataStripeSize
					_, err = nf.ReadAt(newBlobBuf[s], offset)
					if err != nil && err != io.EOF {
						return err
					}
					newData, err := e.enc.Split(newBlobBuf[s])
					if err != nil {
						return err
					}
					// compare
					diffIdx, err := compareStripe(oldData[0:e.K], newData[0:e.K])
					if err != nil {
						return err
					}
					// if no data has been changed,
					if diffIdx == nil {
						return nil
					}
					// we create the argments of Update
					shards := make([][]byte, e.K+e.M)
					for i := range shards {
						shards[i] = make([]byte, e.BlockSize)
					}
					for i := range oldData {
						if i >= e.K || sort.SearchInts(diffIdx, i) != len(diffIdx) {
							copy(shards[i], oldData[i])
						} else {
							shards[i] = nil
							newData[i] = nil
						}
					}
					// update
					err = e.enc.Update(shards, newData[0:e.K])
					if err != nil {
						return err
					}
					// we write back the changed data blocks and all parity blocks
					for i := 0; i < e.K+e.M; i++ {
						i := i
						if shards[i] == nil {
							continue
						}
						newBlock := make([]byte, e.BlockSize)
						if i >= e.K {
							copy(newBlock, shards[i])
						} else {
							copy(newBlock, newData[i])
						}
						erg.Go(func() error {
							diskID := fi.Distribution[stripeNo][i]
							offset := fi.blockToOffset[stripeNo][i]
							_, err := ifs[diskID].WriteAt(newBlock, int64(offset)*e.BlockSize)
							if err != nil {
								return err
							}
							return nil
						})
					}
					if err := erg.Wait(); err != nil {
						return err
					}
					return nil
				})
			} else {
				// if new filesize is greater than old filesize, we just encode the remaining data
				// fmt.Println("new")
				eg.Go(func() error {
					offset := int64(stripeNo) * e.dataStripeSize
					_, err = nf.ReadAt(newBlobBuf[s], offset)
					if err != nil && err != io.EOF {
						return err
					}
					newData, err := e.enc.Split(newBlobBuf[s])
					if err != nil {
						return err
					}
					err = e.enc.Encode(newData)
					if err != nil {
						return err
					}
					erg := e.errgroupPool.Get().(*errgroup.Group)
					defer e.errgroupPool.Put(erg)
					for i := 0; i < e.K+e.M; i++ {
						i := i
						erg.Go(func() error {
							a := i
							diskID := fi.Distribution[stripeNo][a]
							writeOffset := fi.blockToOffset[stripeNo][a]
							_, err := ifs[diskID].WriteAt(newData[a], int64(writeOffset)*e.BlockSize)
							if err != nil {
								return err
							}
							return nil
						})
					}
					if err := erg.Wait(); err != nil {
						return err
					}
					return nil
				})
			}

		}
		if err := eg.Wait(); err != nil {
			return err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}

	if !e.Quiet {
		log.Println(baseName, " successfully updated.")
	}

	return nil
}

// compare the oldStripe and newStripe and return the different blocks' index
// if oldStripe == newStripe, return nil
func compareStripe(oldStripe, newStripe [][]byte) ([]int, error) {
	if len(oldStripe) != len(newStripe) {
		return nil, errors.New("compare error: Invalid Input")
	}
	res := make([]int, 0)
	for i := range oldStripe {
		if !bytes.Equal(oldStripe[i], newStripe[i]) {
			res = append(res, i)
		}
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res, nil
}

func adjustDist(e *Erasure, fi *FileInfo, oldStripeNum, newStripeNum int) {
	countSum := make([]int, e.DiskNum)
	if newStripeNum > oldStripeNum {
		for i := 0; i < newStripeNum-oldStripeNum; i++ {
			fi.Distribution = append(fi.Distribution, make([]int, e.K+e.M))
			fi.blockToOffset = append(fi.blockToOffset, make([]int, e.K+e.M))
		}
		for i := 0; i < oldStripeNum; i++ {
			for j := 0; j < e.K+e.M; j++ {
				diskId := fi.Distribution[i][j]
				countSum[diskId]++
			}
		}
		for i := oldStripeNum; i < newStripeNum; i++ {
			fi.Distribution[i] = genRandomArr(e.DiskNum, 0)[0 : e.K+e.M]
			for j := 0; j < e.K+e.M; j++ {
				diskID := fi.Distribution[i][j]
				fi.blockToOffset[i][j] = countSum[diskID]
				countSum[diskID]++
			}
		}
	} else {
		fi.Distribution = fi.Distribution[0:newStripeNum]
		fi.blockToOffset = fi.blockToOffset[0:newStripeNum]
	}
}
