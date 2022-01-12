package grasure

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/DurantVivado/reedsolomon"
	"golang.org/x/sync/errgroup"
)

//ReadFile reads ONE file  on the system and save it to local `savePath`.
//
//In case of any failure within fault tolerance, the file will be decoded first.
//`degrade` indicates whether degraded read is enabled.
func (e *Erasure) ReadFile(filename string, savepath string, options *Options) error {
	baseFileName := filepath.Base(filename)
	intFi, ok := e.fileMap.Load(baseFileName)
	if !ok {
		return errFileNotFound
	}
	fi := intFi.(*fileInfo)

	fileSize := fi.FileSize
	stripeNum := int(ceilFracInt64(fileSize, e.dataStripeSize))
	dist := fi.Distribution
	//first we check the number of alive disks
	// to judge if any part need reconstruction
	alive := int32(0)
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)

	for i, disk := range e.diskInfos[:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseFileName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				return &diskError{disk.diskPath, " available flag set false"}
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
		if !e.Quiet {
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
		return errTooFewDisksAlive
	}
	if int(alive) == e.DiskNum {
		if !e.Quiet {
			log.Println("start reading blocks")
		}
	} else {
		if !e.Quiet {
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
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	stripeCnt := 0
	nextStripe := 0
	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			// offset := int64(subCnt) * e.allStripeSize
			eg.Go(func() error {
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				//read all blocks in parallel
				//We only have to read k blocks to rec
				failList := make(map[int]bool)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[stripeNo][i]
					disk := e.diskInfos[diskId]
					blkStat := fi.blockInfos[stripeNo][i]
					if !disk.available || blkStat.bstat != blkOK {
						failList[diskId] = true
						continue
					}
					erg.Go(func() error {

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
				splitData, err := e.splitStripe(blobBuf[s])
				if err != nil {
					return err
				}
				//verify and reconstruct if broken
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					// fmt.Println("reconstruct data of stripe:", stripeNo)
					err = e.enc.ReconstructWithList(splitData,
						&failList,
						&(fi.Distribution[stripeNo]),
						options.Degrade)

					// err = e.enc.ReconstructWithKBlocks(splitData,
					// 	&failList,
					// 	&loadBalancedScheme[stripeNo],
					// 	&(fi.Distribution[stripeNo]),
					// 	degrade)
					if err != nil {
						return err
					}
				}
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
				return nil
			})

		}
		if err := eg.Wait(); err != nil {
			return err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe

	}
	if !e.Quiet {
		log.Printf("reading %s...", filename)
	}
	return nil
}

func (e *Erasure) splitStripe(data []byte) ([][]byte, error) {
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
