package main

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
func (e *Erasure) update(oldFile, newFile string) error {
	// read old file info
	baseName := filepath.Base(newFile)
	fi, ok := e.fileMap[baseName]
	if !ok {
		return errFileNotFound
	}
	// update file info
	nf, err := os.Open(newFile)
	if err != nil {
		return err
	}
	defer nf.Close()
	fileInfo, err := nf.Stat()
	oldFileSize := fi.fileSize
	fi.fileSize = fileInfo.Size()
	hashStr, err := hashStr(nf)
	if err != nil {
		return err
	}
	fi.hash = hashStr

	// open file as io.Reader
	alive := int32(0)
	ifs := make([]*os.File, e.k+e.m)
	erg := new(errgroup.Group)
	for i, disk := range e.diskInfos {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				return &DiskError{disk.diskPath, " avilable flag set flase"}
			}
			ifs[i], err = os.OpenFile(blobPath, os.O_RDWR, 0666)
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
		log.Printf("read failed %s:", err.Error())
	}
	defer func() {
		for i := 0; i < e.k+e.m; i++ {
			ifs[i].Close()
		}
	}()
	if int(alive) < e.k {
		//the disk renders inrecoverable
		return ErrTooFewDisks
	}
	if int(alive) == e.k+e.m {
		log.Println("start reading blocks")
	} else {
		log.Println("start reconstructing blocks")
	}

	e.allBlobPool.New = func() interface{} {
		out := make([][]byte, conStripes)
		for i := range out {
			out[i] = make([]byte, e.allStripeSize)
		}
		return &out
	}
	e.dataBlobPool.New = func() interface{} {
		out := make([][]byte, conStripes)
		for i := range out {
			out[i] = make([]byte, e.dataStripeSize)
		}
		return &out
	}
	oldStripeNum := int(ceilFracInt64(oldFileSize, e.dataStripeSize))
	newStripeNum := int(ceilFracInt64(fi.fileSize, e.dataStripeSize))
	numBlob := ceilFracInt(newStripeNum, e.conStripes)
	if newStripeNum > oldStripeNum {
		for i := 0; i < newStripeNum-oldStripeNum; i++ {
			fi.distribution = append(fi.distribution, make([]int, e.k+e.m))
		}
	}
	stripeCnt := 0
	nextStripe := 0
	dist := fi.distribution
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.conStripes > newStripeNum {
			nextStripe = newStripeNum - stripeCnt
		} else {
			nextStripe = e.conStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		newBlobBuf := *e.dataBlobPool.Get().(*[][]byte)
		oldBlobBuf := *e.allBlobPool.Get().(*[][]byte)
		for s := 0; s < nextStripe; s++ {
			s := s
			subCnt := blob*e.conStripes + s
			if subCnt < oldStripeNum {
				// read old data shards
				eg.Go(func() error {
					erg := e.errgroupPool.Get().(*errgroup.Group)
					defer e.errgroupPool.Put(erg)
					for i, disk := range e.diskInfos {
						i := i
						disk := disk
						block := dist[subCnt][i]
						erg.Go(func() error {
							if !disk.available {
								return nil
							}
							_, err := ifs[i].ReadAt(oldBlobBuf[s][int64(block)*e.blockSize:int64(block+1)*e.blockSize],
								int64(stripeCnt+s)*e.blockSize)
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
						// fmt.Println("reconstruct data of stripe:", subCnt)
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
					diffIdx, err := compare(oldData[0:e.k], newData[0:e.k])
					if err != nil {
						return err
					}
					// if no data has been changed,
					if diffIdx == nil {
						return nil
					}
					// we create the argments of Update
					shards := make([][]byte, e.k+e.m)
					for i := range shards {
						shards[i] = make([]byte, blockSize)
					}
					for i := range oldData {
						if i >= e.k || sort.SearchInts(diffIdx, i) != len(diffIdx) {
							copy(shards[i], oldData[i])
						} else {
							shards[i] = nil
							newData[i] = nil
						}
					}
					// update
					err = e.enc.Update(shards, newData[0:e.k])
					if err != nil {
						return err
					}
					// we write back the changed data blocks and all parity blocks
					for i := range e.diskInfos {
						i := i
						j := dist[subCnt][i]
						if shards[j] == nil {
							continue
						}
						newBlock := make([]byte, e.blockSize)
						if j >= e.k {
							copy(newBlock, shards[j])
						} else {
							copy(newBlock, newData[j])
						}
						erg.Go(func() error {
							_, err := ifs[i].WriteAt(newBlock, int64(stripeCnt+s)*e.blockSize)
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
				eg.Go(func() error {
					offset := int64(stripeCnt+s) * e.dataStripeSize
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
					randDist := genRandomArr(e.k + e.m)
					fi.distribution[stripeCnt+s] = randDist
					erg := e.errgroupPool.Get().(*errgroup.Group)
					defer e.errgroupPool.Put(erg)
					for i := range e.diskInfos {
						i := i
						j := randDist[i]
						erg.Go(func() error {
							_, err := ifs[i].WriteAt(newData[j], int64(stripeCnt+s)*e.blockSize)
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
		e.dataBlobPool.Put(&newBlobBuf)
		e.allBlobPool.Put(&oldBlobBuf)
		stripeCnt += nextStripe
	}

	return nil
}

// compare the oldStripe and newStripe and return the different blocks' index
// if oldStripe == newStripe, return nil
func compare(oldStripe, newStripe [][]byte) ([]int, error) {
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
