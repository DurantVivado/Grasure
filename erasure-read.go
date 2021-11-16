package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

//read inputData and return as the io.Reader
func openInput(dataShards, parShards int, fname string) (r []io.Reader, size int64, err error) {
	// Create shards and load the data.
	shards := make([]io.Reader, dataShards+parShards)
	for i := range shards {
		infn := fmt.Sprintf("%s.%d", fname, i)
		fmt.Println("Opening", infn)
		f, err := os.Open(infn)
		if err != nil {
			fmt.Println("Error reading file", err)
			shards[i] = nil
			continue
		} else {
			shards[i] = f
		}
		stat, err := f.Stat()
		if err != nil {
			return nil, 0, err
		}
		if stat.Size() > 0 {
			size = stat.Size()
		} else {
			shards[i] = nil
		}
	}
	return shards, size, nil
}

//read file on the system and return byte stream, include recovering
func (e *Erasure) readFile(filename string, savepath string) error {

	baseFileName := filepath.Base(filename)
	fi, ok := e.fileMap[baseFileName]
	if !ok {
		return ErrFileNotFound
	}

	fileSize := fi.fileSize
	stripeSize := e.dataStripeSize()
	stripeNum := ceilFracInt64(fileSize, stripeSize)
	dist := fi.distribution
	//first we detect the number of alive disks
	alive := int32(0)
	ifs := make([]*os.File, e.k+e.m)
	erg := new(errgroup.Group)
	for i, disk := range e.diskInfos {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				return &DiskError{disk.diskPath, " avilable flag unset"}
			}
			ifs[i], err = os.Open(blobPath)
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
	if int(alive) < e.k {
		//the disk renders inrecoverable
		return ErrTooFewDisks
	}
	if int(alive) == e.k+e.m {
		log.Println("start reading blocks")
	} else {
		log.Println("start reconstructing blocks")
	}
	//for local save path
	sf, err := os.OpenFile(savepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer sf.Close()

	//Since the file is striped, we have to reconstruct each stripe
	//for each stripe we rejoin the data
	numGoroutine := stripeNum
	if numGoroutine > maxGoroutines {
		numGoroutine = maxGoroutines
	}
	// seqChan := make([]chan struct{}, numGoroutine)
	eg := new(errgroup.Group)
	for offset := int64(0); offset < fileSize; offset += stripeSize {
		offset := offset
		eg.Go(func() error {

			g := new(errgroup.Group)
			stripeData := make([][]byte, e.k+e.m)
			//read all blocks in parallel
			for i, disk := range e.diskInfos {
				i := i
				disk := disk
				g.Go(func() error {
					if !disk.available {
						stripeData[dist[offset/stripeSize][i]] = nil
						return nil
					}
					tempBuf := make([]byte, blockSize)
					_, err := ifs[i].Read(tempBuf)
					if err != nil && err != io.EOF {
						return err
					}
					stripeData[dist[offset/stripeSize][i]] = tempBuf
					//then write it to sf
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				return err
			}
			//reconstruct
			ok, err := e.enc.Verify(stripeData)
			if err != nil {
				return err
			}
			if !ok {
				err = e.enc.Reconstruct(stripeData)
				if err != nil {
					return err
				}

			}
			//join and Write
			// if offset > stripeSize {
			// 	<-seqChan[offset/stripeSize-1]
			// }
			for i := 0; i < e.k; i++ {
				if offset+int64(i+1)*blockSize < fileSize {
					_, err := sf.Write(stripeData[i])
					if err != nil {
						return err
					}
				} else { //if remainder is less than one-block length
					leftLen := (fileSize - offset) % blockSize
					_, err := sf.Write(stripeData[i][:leftLen])
					if err != nil {
						return err
					}
					break
				}
				sf.Sync()
			}
			// seqChan[offset/stripeSize] <- struct{}{}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil
	}
	log.Printf("%s successfully read !", filename)

	return nil
}
