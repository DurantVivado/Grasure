package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
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
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	hashStr := fmt.Sprintf("%x", h.Sum(nil))
	f.Seek(0, 0)
	fi := &FileInfo{}
	fi.hash = hashStr
	fi.fileName = baseFileName
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	//we allocate the buffer actually the same size of the file
	// fmt.Println(fileInfo)
	fileSize := fileInfo.Size()
	fi.fileSize = fileSize
	data := make([]byte, fileSize)
	//for blocks...

	buf := bufio.NewReader(f)
	_, err = buf.Read(data)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	//encode the data
	stripeSize := e.dataStripeSize()
	stripeNum := ceilFrac(fileSize, stripeSize)
	fi.distribution = make([][]int, stripeNum)
	//we split file into stripes and randomlu distribute the blocks to various disks
	//and for stripes of the same disk, we concatenate all blocks to create the sole file
	//for accelerating, we start multiple go routine
	//The last stripe will be refilled with zeros
	partData := make([][]byte, len(e.diskInfos))
	partBlock := make([][]int, len(e.diskInfos))
	for size := int64(0); size < fileSize; size += stripeSize {
		stripeData := make([]byte, stripeSize)
		if size+stripeSize > fileSize {
			copy(stripeData, data[size:])
		} else {
			copy(stripeData, data[size:size+stripeSize])
		}

		encodeData, err := e.EncodeData(stripeData)
		if err != nil {
			return nil, err
		}
		//verify the data
		ok, err := e.enc.Verify(encodeData)
		if !ok || err != nil {
			return nil, err
		}
		//generate random distrinution for data and parity
		randDist := genRandomArr(e.k + e.m)
		fi.distribution[size/stripeSize] = randDist
		for i := range e.diskInfos {
			partData[i] = append(partData[i], encodeData[randDist[i]]...)
			partBlock[i] = append(partBlock[i], randDist[i])
		}
	}
	erg := new(errgroup.Group)
	//save the blob
	for i := range e.diskInfos {
		i := i
		//we have to make sure the dist is appended to fi.distribution in order
		func() error {
			folderPath := e.diskInfos[i].diskPath + "/" + baseFileName
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
			partPath := folderPath + "/BLOB"
			//Create the file and write in the parted data
			nf, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return err
			}
			defer nf.Close()
			buf := bufio.NewWriter(nf)
			_, err = buf.Write(partData[i])
			if err != nil {
				return err
			}
			h := sha256.New()
			nf.Seek(0, 0)
			if _, err := io.Copy(h, nf); err != nil {
				return err
			}
			hashStr = fmt.Sprintf("%x\n%v\n", h.Sum(nil), partBlock[i])

			nf.Sync()
			buf.Flush()
			//for meta information:
			//we store:1. which blocks are in this part and 2. hashstr for checking integrity
			metaPath := folderPath + "/META"
			//Create the file and write in the hash
			cf, err := os.OpenFile(metaPath, os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				return err
			}
			defer cf.Close()
			buf = bufio.NewWriter(cf)
			_, err = buf.WriteString(hashStr)
			if err != nil {
				return err
			}
			cf.Sync()
			buf.Flush()

			return nil
		}()

	}

	if err := erg.Wait(); err != nil {
		return nil, err
	}

	//record the file meta
	e.fileMap[baseFileName] = fi
	log.Println(baseFileName, " successfully encoded.")
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

//allStripe contains both data and parity blocks
func (e *Erasure) allStripeSize() int64 {
	return e.blockSize * int64(e.k+e.m)
}

//dataStripe contains only data
func (e *Erasure) dataStripeSize() int64 {
	return e.blockSize * int64(e.k)
}

//return final erasure size from original size,
//Every block spans all the data disks and split into shards
//the shardSize is the same except for the last one
func (e *Erasure) stripedFileSize(totalLen int64) int64 {
	if totalLen <= 0 {
		return 0
	}
	dataStripeSize := e.dataStripeSize()
	numBlocks := totalLen / dataStripeSize
	lastStripeSize := totalLen % dataStripeSize
	lastBlockSize := ceilFrac(lastStripeSize, e.blockSize)
	return numBlocks*e.blockSize + lastBlockSize

}
