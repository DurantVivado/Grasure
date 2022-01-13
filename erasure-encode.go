package grasure

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

//EncodeFile takes filepath as input and encodes the file into data and parity blocks concurrently.
//
// It returns `*fileInfo` and an error. Specify `blocksize` and `conStripe` for better performance.
func (e *Erasure) EncodeFile(filename string) (*fileInfo, error) {
	baseFileName := filepath.Base(filename)
	if _, ok := e.fileMap.Load(baseFileName); ok && !e.Override {
		return nil, fmt.Errorf("the file %s has already been in the file system, if you wish to override, please attach `-o`",
			baseFileName)
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
	fi := &fileInfo{}
	fi.FileId = int64(len(e.FileMeta))
	fi.Hash = hashStr
	fi.FileName = baseFileName
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fi.FileSize = fileSize
	//how much data read in a batch is worth discussion
	//for blocks...

	//encode the data
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	//we split file into stripes and randomly distribute the blocks to various disks
	//and for stripes of the same disk, we concatenate all blocks to create the sole file
	//for accelerating, we start multiple goroutines
	//The last stripe will be refilled with zeros
	of := make([]*os.File, e.DiskNum)
	//first open relevant file resources
	erg := new(errgroup.Group)
	//save the blob
	for i := range e.diskInfos[:e.DiskNum] {
		i := i
		//we have to make sure the dist is appended to fi.Distribution in order
		erg.Go(func() error {
			folderPath := filepath.Join(e.diskInfos[i].diskPath, baseFileName)
			//if override is specified, we override previous data
			if e.Override {
				if err := os.RemoveAll(folderPath); err != nil {
					return err
				}
			}
			if err := os.Mkdir(folderPath, 0666); err != nil {
				return errDataDirExist
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
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	stripeCnt := 0
	//for every conStripe stripes, we set one goroutine
	nextStripe := 0
	//allocate the memory pool only when needed
	// e.dataBlobPool.New = func() interface{} {
	// 	out := make([][]byte, e.ConStripes)
	// 	for i := range out {
	// 		out[i] = make([]byte, e.dataStripeSize)
	// 	}
	// 	return &out
	// }
	//we make layout independent of encoding and user-friendly
	//all described in erasure-layout.go

	e.generateLayout(fi)
	blobBuf := makeArr2DByte(e.ConStripes, int(e.dataStripeSize))
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		// blobBuf := *e.dataBlobPool.Get().(*[][]byte)
		for s := 0; s < nextStripe; s++ {
			s := s
			offset := int64(stripeCnt+s) * e.dataStripeSize
			eg.Go(func() error {
				_, err := f.ReadAt(blobBuf[s], offset)
				if err != nil && err != io.EOF {
					return err
				}
				//split and encode the data
				encodeData, err := e.encodeData(blobBuf[s])
				if err != nil {
					return err
				}
				//generate random distrinution for data and parity
				randDist := fi.Distribution[stripeCnt+s]
				// randDist := getSeqArr(e.K + e.M)

				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//save the blob
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := randDist[i]
					erg.Go(func() error {
						offset := fi.blockToOffset[stripeCnt+s][i]
						_, err := of[diskId].WriteAt(encodeData[i], int64(offset)*e.BlockSize)
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
		// e.dataBlobPool.Put(&blobBuf)
		stripeCnt += nextStripe

	}
	for i := range of {
		of[i].Close()
	}
	//record the file meta
	//transform map into array for json marshaling
	e.fileMap.Store(baseFileName, fi)
	fi.blockInfos = make([][]*blockInfo, stripeNum)
	e.stripeNum += int64(stripeNum)
	for row := range fi.Distribution {
		fi.blockInfos[row] = make([]*blockInfo, e.K+e.M)
		for line := range fi.Distribution[row] {
			fi.blockInfos[row][line] = &blockInfo{bstat: blkOK}
		}
	}
	// e.fileMap[baseFileName] = fi
	if !e.Quiet {
		log.Println(baseFileName, " successfully encoded. encoding size ",
			e.stripedFileSize(fileSize), "bytes")
	}
	return fi, nil
}

//split and encode data
func (e *Erasure) encodeData(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.K+e.M), nil
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
