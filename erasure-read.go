package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/DurantVivado/reedsolomon"
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
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.distribution
	//first we check the number of alive disks
	// to judge if any part need reconstruction
	alive := int32(0)
	ifs := make([]*os.File, e.k+e.m)
	erg := new(errgroup.Group)
	diskFailList := make(map[int]bool)
	for i, disk := range e.diskInfos {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				diskFailList[i] = true
				return &DiskError{disk.diskPath, " available flag set false"}
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
	//for local save path
	sf, err := os.OpenFile(savepath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer sf.Close()

	//Since the file is striped, we have to reconstruct each stripe
	//for each stripe we rejoin the data
	numBlob := ceilFracInt(stripeNum, e.conStripes)
	stripeCnt := 0
	nextStripe := 0
	//allocate stripe-size pool if and only if needed
	e.allBlobPool.New = func() interface{} {
		out := make([][]byte, conStripes)
		for i := range out {
			out[i] = make([]byte, e.allStripeSize)
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
		blobBuf := *e.allBlobPool.Get().(*[][]byte)
		for s := 0; s < nextStripe; s++ {
			s := s
			subCnt := blob*e.conStripes + s
			// offset := int64(subCnt) * e.allStripeSize
			func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//read all blocks in parallel
				for i, disk := range e.diskInfos {
					i := i
					disk := disk
					block := dist[subCnt][i]
					erg.Go(func() error {
						if !disk.available {
							return nil
						}
						if ifs[i] == nil {
							return nil
						}
						_, err := ifs[i].ReadAt(blobBuf[s][int64(block)*e.blockSize:int64(block+1)*e.blockSize],
							int64(stripeCnt+s)*e.blockSize)
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
				splitData, err := e.SplitStripe(blobBuf[s])
				if err != nil {
					return err
				}
				//verify and reconstruct
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					// fmt.Println("reconstruct data of stripe:", subCnt)
					err = e.enc.ReconstructWithList(splitData, &diskFailList, &(fi.distribution[stripeCnt+s]), false)
					if err != nil {
						return err
					}
				}
				//join and write to output file

				for i := 0; i < e.k; i++ {
					i := i
					writeOffset := int64(stripeCnt+s)*e.dataStripeSize + int64(i)*blockSize
					if fileSize-writeOffset <= blockSize {
						leftLen := fileSize - writeOffset
						_, err := sf.WriteAt(splitData[i][:leftLen], writeOffset)
						if err != nil {
							return err
						}
						break
					}
					erg.Go(func() error {
						// fmt.Println("i:", i, "writeOffset", writeOffset+e.blockSize, "at stripe", subCnt)
						_, err := sf.WriteAt(splitData[i], writeOffset)
						if err != nil {
							return err
						}
						// sf.Sync()
						return nil
					})

				}
				if err := erg.Wait(); err != nil {
					return err
				}
				return err
			}()

		}
		e.allBlobPool.Put(&blobBuf)
		if err := eg.Wait(); err != nil {
			return err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe

	}
	log.Printf("reading %s!", filename)
	return nil
}

func (e *Erasure) SplitStripe(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, reedsolomon.ErrShortData
	}
	// Calculate number of bytes per data shard.
	perShard := ceilFracInt(len(data), e.k+e.m)

	// Split into equal-length shards.
	dst := make([][]byte, e.k+e.m)
	i := 0
	for ; i < len(dst) && len(data) >= perShard; i++ {
		dst[i], data = data[:perShard:perShard], data[perShard:]
	}

	return dst, nil
}
