package grasure

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

//ReadFile reads ONE file  on the system and save it to local `savePath`.
//
//In case of any failure within fault tolerance, the file will be decoded first.
//`Degrade` indicates whether degraded read is enabled.
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
	//---------------------------------------
	//stripeOrder arranges the repairing order of each stripe
	//i.e., stripe[t] contains the failed stripe(s) that could be
	// recover in the t_th time slice. So on and so forth.
	var stripeOrder map[int][]int
	//---------------------------------------
	if int(alive) == e.DiskNum {
		if !e.Quiet {
			log.Println("start reading blocks")
		}
	} else {
		if !e.Quiet {
			log.Println("start reconstructing blocks")
		}
		//--------------------------------
		if options.WithSGA {
			fi.loadBalancedScheme, stripeOrder, err = e.SGA(fi, options.WithGCA)
			if err != nil {
				return err
			}
		}
		//-------------------------------
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
	//-----------------------------------
	//diskLoads records the load level of each disks(in blocks).
	diskLoads := make([]int32, e.DiskNum)
	//-----------------------------------

	//Without SGA: for every stripe pick up the first k alive blocks for repairing
	//With SGA: for maximal load-balance, pick up k alive blocks chosen by SGA
	//
	//Without GCA: the stripes are repaired in consecutive order
	//With GCA:  the stripes are repaired concurrently so that the total time slice is minimized
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
			func() error {
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
				//the failed ones are left to next step
				if !ok && !options.WithGCA {

					if options.WithSGA {
						err = e.enc.ReconstructWithKBlocks(splitData,
							&failList,
							&fi.loadBalancedScheme[stripeNo],
							&(fi.Distribution[stripeNo]),
							options.Degrade)
					} else {
						err = e.enc.ReconstructWithList(splitData,
							&failList,
							&(fi.Distribution[stripeNo]),
							options.Degrade)
					}
					if err != nil {
						return err
					}
					tempCnt := 0
					for _, disk := range dist[stripeNo] {
						if _, ok := failList[disk]; !ok {
							atomic.AddInt32(&diskLoads[disk], 1)
							tempCnt++
							if tempCnt >= e.K {
								break
							}
						}
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
			}()

		}
		if err := eg.Wait(); err != nil {
			return err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}
	if options.WithGCA {
		//the reading upheld by GCA algorithm
		minTimeSlice := len(stripeOrder)
		for t := 1; t <= minTimeSlice; t++ {
			eg := e.errgroupPool.Get().(*errgroup.Group)
			strps := stripeOrder[t]
			blobBuf := makeArr2DByte(len(strps), int(e.allStripeSize))
			for s, stripeNo := range strps {
				stripeNo := stripeNo
				s := s
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
					//must be broken so verifying is needless
					// ok, err := e.enc.Verify(splitData)
					// if err != nil {
					// 	return err
					// }
					// if !ok {

					err = e.enc.ReconstructWithKBlocks(splitData,
						&failList,
						&fi.loadBalancedScheme[stripeNo],
						&(fi.Distribution[stripeNo]),
						options.Degrade)
					if err != nil {
						return err
					}
					//----------------------------------------
					tempCnt := 0
					for _, disk := range fi.loadBalancedScheme[stripeNo] {
						if _, ok := failList[disk]; !ok {
							atomic.AddInt32(&diskLoads[disk], 1)
							tempCnt++
							if tempCnt >= e.K {
								break
							}
						}
					}
					//---------------------------------------
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
			if err := eg.Wait(); err != nil {
				return err
			}
			e.errgroupPool.Put(eg)
		}
	}
	if !e.Quiet {
		//--------------------------------------------
		fmt.Printf("------------------Normal--------------------")
		maxload, sumload := 0, 0
		for i := range diskLoads {
			maxload = max(maxload, int(diskLoads[i]))
			sumload += int(diskLoads[i])
		}
		fmt.Printf("\nmaxLoad:%d, sumLoad: %d\n", maxload, sumload)
		fmt.Printf("disk loads:\n%v\n", diskLoads)
		//-------------------------------------------
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
