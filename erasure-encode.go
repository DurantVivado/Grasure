package main

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
) //split and encode a file into parity blocks concurrently
func (e *Erasure) encode(filename string) (*FileInfo, error) {
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
	data := make([]byte, size)
	buf := bufio.NewReader(f)
	_, err = buf.Read(data)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	//The input file will be split in multiples of a blockSize, 4KiB by default
	//if file size is less than blockSize, the redundant zeros will be padded to the last stripe
	stripeSize := (e.k + e.m) * e.blockSize
	padded_data := make([]byte, (len(data)+stripeSize-1)/stripeSize*stripeSize)
	copy(padded_data, data)
	//we split the data, and create empty parity
	shards, err := e.enc.Split(padded_data)
	if err != nil {
		return nil, err
	}
	// test, err := os.OpenFile("testJoin.txt", os.O_CREATE|os.O_RDWR, 0666)
	// if err != nil {
	// 	return nil, err
	// }
	// defer test.Close()
	// err = e.enc.Join(test, shards, int(size))
	// if err != nil {
	// 	return nil, err
	// }
	// if _, err := io.Copy(h, test); err != nil {
	// 	return nil, err
	// }
	// hashTest := fmt.Sprintf("%x", h.Sum(nil))
	// if strings.Compare(hashStr, hashTest) == 0 {
	// 	fmt.Println("ok")
	// }
	//encode the data
	err = e.enc.Encode(shards)
	if err != nil {
		return nil, err
	}
	//verify the data
	ok, err := e.enc.Verify(shards)
	if !ok || err != nil {
		return nil, err
	}
	//we save the encoded shards to dst
	//Before tht, we shuffle the data and paritys
	numDisks := len(e.diskInfos)
	shuff := make([]int, numDisks)
	for i := 0; i < numDisks; i++ {
		shuff[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
	fi.distribution = shuff
	e.fileMap[filename] = fi
	//we save the data and parity to the mounted data point of each disk
	//we adopt the methods of Minio, which first create a folder with the same name as the file,
	//and store each part of the file in the folder with name like "D_X" or "P_X".
	//Remember these files are seperable, use byte slices to ignite your desiderata
	//create folder with name of the file
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

	return fi, nil
}
