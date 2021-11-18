package main

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
) //split and encode a file into parity blocks concurrently

func (e *Erasure) EncodeFile(ctx context.Context, filename string) (*FileInfo, error) {
	baseFileName := filepath.Base(filename)
	if _, ok := e.fileMap[baseFileName]; ok && !override {
		log.Fatalf("the file %s has already been in HDR file system, you should update instead of encoding", baseFileName)
		return nil, nil
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	//We sum the hash of the file
	hashStr, err := hashStr(f)
	if err != nil {
		return nil, err
	}
	f.Seek(0, 0)
	fi := &FileInfo{}
	fi.hash = hashStr
	fi.fileName = baseFileName
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fi.fileSize = fileSize
	//how much data read in a batch is worth discussion
	//for blocks...

	//encode the data
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	fi.distribution = make([][]int, stripeNum)
	//we split file into stripes and randomly distribute the blocks to various disks
	//and for stripes of the same disk, we concatenate all blocks to create the sole file
	//for accelerating, we start multiple goroutines
	//The last stripe will be refilled with zeros
	// partBlock := make([][]int, len(e.diskInfos))
	// buf := bufio.NewReader(f)
	of := make([]*os.File, e.k+e.m)
	//first open relevant file resources
	erg := new(errgroup.Group)
	//save the blob
	for i := range e.diskInfos {
		i := i
		//we have to make sure the dist is appended to fi.distribution in order
		erg.Go(func() error {
			folderPath := filepath.Join(e.diskInfos[i].diskPath, baseFileName)
			//if override is specified, we override previous data
			if override {
				if err := os.RemoveAll(folderPath); err != nil {
					return err
				}
			}
			if err := os.Mkdir(folderPath, 0666); err != nil {
				return ErrDataDirExist
			}
			// We decide the part name according to whether it belongs to data or parity
			partPath := filepath.Join(folderPath, "BLOB")
			//Create the file and write in the parted data
			of[i], err = os.OpenFile(partPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		return nil, err
	}
	numBlob := ceilFracInt(stripeNum, e.conStripes)
	stripeCnt := 0
	//for every conStripe stripes, we set one goroutine
	nextStripe := 0
	//allocate the memory pool only when needed
	e.dataBlobPool.New = func() interface{} {
		out := make([][]byte, conStripes)
		for i := range out {
			out[i] = make([]byte, e.dataStripeSize)
		}
		return &out
	}
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.conStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.conStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		blobBuf := *e.dataBlobPool.Get().(*[][]byte)
		for s := 0; s < nextStripe; s++ {
			s := s
			offset := int64(stripeCnt+s) * e.dataStripeSize
			eg.Go(func() error {
				_, err := f.ReadAt(blobBuf[s], offset)
				if err != nil && err != io.EOF {
					return err
				}
				//split and encode the data
				encodeData, err := e.EncodeData(blobBuf[s])
				if err != nil {
					return err
				}
				//generate random distrinution for data and parity
				randDist := genRandomArr(e.k + e.m)
				// randDist := getSeqArr(e.k + e.m)

				fi.distribution[stripeCnt+s] = randDist

				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//save the blob
				for i := range e.diskInfos {
					i := i
					j := randDist[i]
					erg.Go(func() error {
						_, err := of[i].WriteAt(encodeData[j], int64(stripeCnt+s)*e.blockSize)
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
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
		e.dataBlobPool.Put(&blobBuf)
		stripeCnt += nextStripe

	}
	for i := range of {
		of[i].Close()
	}
	//record the file meta
	e.fileMap[baseFileName] = fi
	log.Println(baseFileName, " successfully encoded. encoding size ", e.stripedFileSize(fileSize), "bytes")
	return fi, nil
}

//split and encode data
func (e *Erasure) EncodeData(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.k+e.m), nil
	}
	encoded, err := e.enc.Split(data)
	if err != nil {
		return nil, err
	}
	if err := e.enc.Encode(encoded); err != nil {
		return nil, err
	}
	return encoded, nil
}

//return final erasure size from original size,
//Every block spans all the data disks and split into shards
//the shardSize is the same except for the last one
func (e *Erasure) stripedFileSize(totalLen int64) int64 {
	if totalLen <= 0 {
		return 0
	}
	numStripe := ceilFracInt64(totalLen, e.dataStripeSize)
	return numStripe * e.allStripeSize

}
