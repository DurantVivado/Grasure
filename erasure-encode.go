package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
) //split and encode a file into parity blocks concurrently
func (e *Erasure) EncodeFile(ctx context.Context, filename string) (*FileInfo, error) {
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
	fi.fileName = filename
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	//we allocate the buffer actually the same size of the file
	// fmt.Println(fileInfo)
	size := fileInfo.Size()
	fi.fileSize = size
	//for blocks...
	size = e.stripedFileSize(size)
	data := make([]byte, size)
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
	stripeNum := ceilFrac(size, stripeSize)
	for i := 0; i < int(stripeNum); i++ {
		stripedData := data[i*int(stripeSize) : (i+1)*int(stripeSize)]

		shards, err := e.EncodeData(ctx, stripedData)
		//verify the data
		ok, err := e.enc.Verify(shards)
		if !ok || err != nil {
			return nil, err
		}
		//we save the encoded shards to dst
		//Before tht, we shuffle the data and paritys
		numDisks := len(e.diskInfos)
		fi.distribution = genRandomArr(numDisks)
		var folderPath, partPath string
		for i, path := range e.diskInfos {
			folderPath = path.diskPath + "/" + filename
			//first we create a folder
			if *override {

				if err := os.RemoveAll(folderPath); err != nil {
					return nil, err
				}

			}
			if err := os.Mkdir(folderPath, 0666); err != nil {
				return nil, ErrDataDirExist
			}
			//We decide the part name according to whether it belongs to data or parity
			if shuff[i] < e.k { //data block
				partPath = folderPath + "/D_" + fmt.Sprintf("%d", shuff[i])
			} else {
				partPath = folderPath + "/P_" + fmt.Sprintf("%d", shuff[i])

			}
			//Create the file and write in the parted data
			nf, err := os.OpenFile(partPath, os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				return nil, err
			}
			defer nf.Close()
			buf := bufio.NewWriter(nf)
			_, err = buf.Write(shards[shuff[i]])
			if err != nil {
				return nil, err
			}
			buf.Flush()

		}

	}
	//record the file meta
	e.fileMap[filename] = fi

	return fi, nil
}

//split and encode data
func (e *Erasure) EncodeData(ctx context.Context, data []byte) ([][]byte, error) {
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
