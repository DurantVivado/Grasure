package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/DurantVivado/reedsolomon"
	"golang.org/x/sync/errgroup"
)

//read file on the system and return byte stream, include recovering
func (e *Erasure) readFile(filename string, savepath string) error {
	baseFileName := filepath.Base(filename)
	intFi, ok := e.fileMap.Load(baseFileName)
	fi := intFi.(*FileInfo)
	if !ok {
		return ErrFileNotFound
	}

	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	//first we check the number of alive disks
	// to judge if any part need reconstruction
	alive := int32(0)
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)
	diskFailList := make(map[int]bool)
	for i, disk := range e.diskInfos[:e.DiskNum] {
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
		if !e.quiet {
			log.Printf("%s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < e.DiskNum; i++ {
			if ifs[i] != nil {
				ifs[i].Close()
			}
		}
	}()
	if int(alive) < e.K {
		//the disk renders inrecoverable
		return ErrTooFewDisksAlive
	}
	if int(alive) == e.DiskNum {
		if !e.quiet {
			log.Println("start reading blocks")
		}
	} else {
		if !e.quiet {
			log.Println("start reconstructing blocks")
		}
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
	// e.allBlobPool.New = func() interface{} {
	// 	out := make([][]byte, e.conStripes)
	// 	for i := range out {
	// 		out[i] = make([]byte, e.allStripeSize)
	// 	}
	// 	return &out
	// }
	blobBuf := makeArr2DByte(e.conStripes, int(e.allStripeSize))
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.conStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.conStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		// blobBuf := *e.allBlobPool.Get().(*[][]byte)
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			// offset := int64(subCnt) * e.allStripeSize
			eg.Go(func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//read all blocks in parallel
				for i := 0; i < e.K; i++ {
					i := i
					diskId := dist[stripeNo][i]
					disk := e.diskInfos[diskId]
					erg.Go(func() error {
						if !disk.available {
							return nil
						}
						//we also need to know the block's accurate offset with respect to disk
						offset := fi.blockToOffset[stripeNo][i]
						_, err := ifs[diskId].ReadAt(blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
							int64(offset)*e.BlockSize)
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
				// ok, err := e.enc.Verify(splitData)
				// if err != nil {
				// 	return err
				// }
				// if !ok {
				// fmt.Println("reconstruct data of stripe:", stripeNo)
				err = e.enc.ReconstructWithList(splitData, &diskFailList, &(fi.Distribution[stripeNo]), false)
				if err != nil {
					return err
				}
				// }
				//join and write to output file

				for i := 0; i < e.K; i++ {
					i := i
					writeOffset := int64(stripeNo)*e.dataStripeSize + int64(i)*e.BlockSize
					if fileSize-writeOffset <= e.BlockSize {
						leftLen := fileSize - writeOffset
						_, err := sf.WriteAt(splitData[i][:leftLen], writeOffset)
						if err != nil {
							return err
						}
						break
					}
					erg.Go(func() error {
						// fmt.Println("i:", i, "writeOffset", writeOffset+e.BlockSize, "at stripe", subCnt)
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
			})

		}
		// e.allBlobPool.Put(&blobBuf)
		if err := eg.Wait(); err != nil {
			return err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe

	}
	if !e.quiet {
		log.Printf("reading %s!", filename)
	}
	return nil
}

func (e *Erasure) SplitStripe(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, reedsolomon.ErrShortData
	}
	// Calculate number of bytes per data shard.
	perShard := ceilFracInt(len(data), e.K+e.M)

	// Split into equal-length shards.
	dst := make([][]byte, e.K+e.M)
	i := 0
	for ; i < len(dst) && len(data) >= perShard; i++ {
		dst[i], data = data[:perShard:perShard], data[perShard:]
	}

	return dst, nil
}
