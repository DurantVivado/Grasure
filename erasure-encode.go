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

	"github.com/klauspost/reedsolomon"
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
	//how much data read in a batch is worth discussion
	//for blocks...

	//encode the data
	stripeSize := e.dataStripeSize()
	stripeNum := ceilFrac(fileSize, stripeSize)
	fi.distribution = make([][]int, stripeNum)
	//we split file into stripes and randomlu distribute the blocks to various disks
	//and for stripes of the same disk, we concatenate all blocks to create the sole file
	//for accelerating, we start multiple go routine
	//The last stripe will be refilled with zeros
	partBlock := make([][]int, len(e.diskInfos))
	// buf := bufio.NewReader(f)
	stripeno := int64(0)
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
			of[i], err = os.OpenFile(partPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0754)
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		return nil, err
	}

	for {
		data := make([]byte, e.dataStripeSize())
		bytes, err := f.Read(data)
		if err != nil && err != io.EOF {
			return nil, err
		}

		encodeData, err := e.EncodeData(data)
		if err != nil {
			return nil, err
		}
		//generate random distrinution for data and parity
		randDist := genRandomArr(e.k + e.m)
		fi.distribution[stripeno] = randDist
		stripeno++
		for i := range e.diskInfos {
			partBlock[i] = append(partBlock[i], randDist[i])
		}

		erg := new(errgroup.Group)
		//save the blob
		for i := range e.diskInfos {
			i := i
			j := randDist[i]
			//we have to make sure the dist is appended to fi.distribution in order
			erg.Go(func() error {
				_, err = of[i].Write(encodeData[j])
				if err != nil {
					return err
				}
				of[i].Sync()
				// h := sha256.New()
				// nf.Seek(0, 0)
				// if _, err := io.Copy(h, nf); err != nil {
				// 	return err
				// }
				// hashStr = fmt.Sprintf("%x\n%v\n", h.Sum(nil), partBlock[i])

				// nf.Sync()
				// buf.Flush()
				// //for meta information:
				// //we store:1. which blocks are in this part and 2. hashstr for checking integrity
				// metaPath := filepath.Join(folderPath, "META")
				// //Create the file and write in the hash
				// cf, err := os.OpenFile(metaPath, os.O_WRONLY|os.O_CREATE, 0666)
				// if err != nil {
				// 	return err
				// }
				// defer cf.Close()
				// buf = bufio.NewWriter(cf)
				// _, err = buf.WriteString(hashStr)
				// if err != nil {
				// 	return err
				// }
				// cf.Sync()
				// buf.Flush()

				return nil
			})

		}
		if err := erg.Wait(); err != nil {
			return nil, err
		}
		if int64(bytes) < e.dataStripeSize() {
			break
		}
	}
	for i := range of {
		of[i].Close()
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

// split and encode data stream
// Simple Encoder/Decoder Shortcomings:
// * If the file size of the input isn't dividable by the number of data shards
//   the output will contain extra zeroes
//
// * If the shard numbers isn't the same for the decoder as in the
//   encoder, invalid output will be generated.
//
// * If values have changed in a shard, it cannot be reconstructed.
//
// * If two shards have been swapped, reconstruction will always fail.
//   You need to supply the shards in the same order as they were given to you.
//
// The solution for this is to save a metadata file containing:
//
// * File size.
// * The number of data/parity shards.
// * HASH of each shard.
// * Order of the shards.
func (e *Erasure) EncodeStreamData(ctx context.Context, filename string) (*FileInfo, error) {
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

	//for outputs, we first create the dest files
	out := make([]*os.File, e.k+e.m)
	for i := range e.diskInfos {
		outfn := fmt.Sprintf("BLOB.%d", i)
		folderPath := filepath.Join(e.diskInfos[i].diskPath, baseFileName)
		blobPath := filepath.Join(folderPath, outfn)
		if override {
			if err := os.RemoveAll(folderPath); err != nil {
				return nil, err
			}
		}
		if err := os.Mkdir(folderPath, 0666); err != nil {
			return nil, ErrDataDirExist
		}
		out[i], err = os.Create(blobPath)
		if err != nil {
			return nil, err
		}
		defer func() {
			//save meta
			out[i].Seek(0, 0)
			h := sha256.New()
			if _, err := io.Copy(h, f); err != nil {
				log.Fatal(err)
			}
			hashStr := fmt.Sprintf("%x", h.Sum(nil))
			outfn := fmt.Sprintf("META.%d", i)
			metaPath := filepath.Join(folderPath, outfn)
			//Create the file and write in the hash
			cf, err := os.OpenFile(metaPath, os.O_WRONLY|os.O_CREATE, 0666)
			if err != nil {
				log.Fatal(err)
			}
			defer cf.Close()
			buf := bufio.NewWriter(cf)
			_, err = buf.WriteString(hashStr)
			if err != nil {
				log.Fatal(err)
			}
			cf.Sync()
			buf.Flush()

		}()
		defer out[i].Close()
	}
	//for large files, everytime we read a stripe of data
	//encode the data
	stripeSize := e.dataStripeSize()
	stripeNum := ceilFrac(fileSize, stripeSize)
	fi.distribution = make([][]int, stripeNum)
	data := make([]io.Writer, e.k)
	input := make([]io.Reader, e.k)
	parity := make([]io.Writer, e.m)
	//reopen the file as io.Reader
	for {
		// for every stripe, generate random layout
		randDist := genRandomArr(e.k + e.m)
		//the data
		for i := range data {
			data[i] = out[randDist[i]]
		}
		//Do the split
		err = e.sEnc.Split(f, data, stripeSize)
		if err == reedsolomon.ErrShortData {
			break
		} else if err != nil {
			return nil, err
		}
		f.Seek(stripeSize, 1)
		//create parity output writers
		for i := range parity {
			j := randDist[e.k+i]
			parity[i] = out[j]

		}
		for i := range data {
			j := randDist[i]
			out[j].Close()
			nf, err := os.Open(out[j].Name())
			if err != nil {
				return nil, err
			}
			input[i] = f
			nf.Close()

		}
		//Encode parity
		err = e.sEnc.Encode(input, parity)
		if err != nil {
			return nil, err
		}
		fi.distribution = append(fi.distribution, randDist)
	}
	//record the file meta
	e.fileMap[baseFileName] = fi
	log.Println(baseFileName, " successfully encoded.")
	return fi, nil
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
