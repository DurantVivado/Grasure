package grasure

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

func (e *Erasure) RecoverWithStripe(fileName string, options *Options) (map[string]string, error) {
	var failDisk int = 0
	for i := range e.diskInfos {
		if !e.diskInfos[i].available {
			failDisk = i
			break
		}
	}
	// fmt.Println(failDisk)
	if !e.Quiet {
		log.Printf("Start recovering with stripe, totally %d stripes need recovery",
			len(e.StripeInDisk[failDisk]))
	}
	// fmt.Println("failDisk:")
	// fmt.Println(failDisk)
	baseName := filepath.Base(fileName)
	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	diskFailList := make(map[int]bool, 1)

	ReplaceMap[e.diskInfos[failDisk].diskPath] = e.diskInfos[e.DiskNum].diskPath
	replaceMap[failDisk] = e.DiskNum
	diskFailList[failDisk] = true

	// start recovering: recover all stripes in this disk

	// open all disks
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)
	// alive := int32(0)
	for i, disk := range e.diskInfos[0:e.DiskNum] {
		i := i
		disk := disk
		erg.Go(func() error {
			folderPath := filepath.Join(disk.diskPath, baseName)
			blobPath := filepath.Join(folderPath, "BLOB")
			if !disk.available {
				ifs[i] = nil
				return nil
			}
			ifs[i], err = os.Open(blobPath)
			if err != nil {
				return err
			}

			disk.available = true
			// atomic.AddInt32(&alive, 1)
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		if !e.Quiet {
			log.Printf("read failed %s", err.Error())
		}
	}
	defer func() {
		for i := 0; i < e.DiskNum; i++ {
			if ifs[i] != nil {
				ifs[i].Close()
			}
		}
	}()
	// if int(alive) < e.K {
	// 	//the disk renders inrecoverable
	// 	return nil, errTooFewDisksAlive
	// }
	// if int(alive) == e.DiskNum {
	// 	if !e.Quiet {
	// 		log.Println("start reading blocks")
	// 	}
	// } else {
	if !e.Quiet {
		log.Println("start reconstructing blocks")
	}
	// }

	// create BLOB in the backup disk
	disk := e.diskInfos[e.DiskNum]
	folderPath := filepath.Join(disk.diskPath, baseName)
	blobPath := filepath.Join(folderPath, "BLOB")
	if e.Override {
		if err := os.RemoveAll(folderPath); err != nil {
			return nil, err
		}
	}
	if err := os.Mkdir(folderPath, 0666); err != nil {
		return nil, errDataDirExist
	}
	rfs, err := os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, err
	}
	defer rfs.Close()

	// read stripes every blob in parallel
	// read blocks every stripe in parallel
	// erg = e.errgroupPool.Get().(*errgroup.Group)
	stripeNum := len(e.StripeInDisk[failDisk])
	numBlob := ceilFracInt(stripeNum, e.ConStripes)
	// fmt.Println(e.ConStripes, numBlob)
	blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
	stripeCnt := 0
	nextStripe := 0
	stripes := e.StripeInDisk[failDisk]
	// fmt.Println("stripeIds:")
	// fmt.Println(stripes)

	for blob := 0; blob < numBlob; blob++ {
		if stripeCnt+e.ConStripes > stripeNum {
			nextStripe = stripeNum - stripeCnt
		} else {
			nextStripe = e.ConStripes
		}
		eg := e.errgroupPool.Get().(*errgroup.Group)
		for s := 0; s < nextStripe; s++ {
			s := s
			stripeNo := stripeCnt + s
			eg.Go(func() error {
				// s := s
				spId := stripes[stripeNo]
				spInfo := e.Stripes[spId]
				erg := e.errgroupPool.Get().(*errgroup.Group)
				defer e.errgroupPool.Put(erg)
				// get dist and blockToOffset by stripeNo
				dist := spInfo.Dist
				blockToOffset := spInfo.BlockToOffset
				// fmt.Println(spId, dist, blockToOffset)
				// read blocks in parallel
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					disk := e.diskInfos[diskId]
					if !disk.available {
						continue
					}
					erg.Go(func() error {
						offset := blockToOffset[i]
						_, err := ifs[diskId].ReadAt(blobBuf[s][int64(i)*e.BlockSize:int64(i+1)*e.BlockSize],
							int64(offset)*e.BlockSize)
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
				ok, err := e.enc.Verify(splitData)
				if err != nil {
					return err
				}
				if !ok {
					err = e.enc.ReconstructWithList(splitData, &diskFailList, &(dist), options.Degrade)
					if err != nil {
						return err
					}
				} else {
					return nil
				}
				//write the Blob to restore paths
				// egp := e.errgroupPool.Get().(*errgroup.Group)
				// defer e.errgroupPool.Put(egp)
				for i := 0; i < e.K+e.M; i++ {
					i := i
					diskId := dist[i]
					// if v, ok := replaceMap[diskId]; ok {
					if diskId == failDisk {
						// restoreId := v - e.DiskNum
						writeOffset := blockToOffset[i]
						// fmt.Println(spId, spInfo.BlockToOffset, blockToOffset)
						// egp.Go(func() error {
						_, err := rfs.WriteAt(splitData[i], int64(writeOffset)*e.BlockSize)
						if err != nil {
							return err
						}
						if e.diskInfos[diskId].ifMetaExist {
							newMetapath := filepath.Join(e.diskInfos[e.DiskNum].diskPath, "META")
							if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
								return err
							}
						}
						break
					}
					// return nil
					// })
					// }
				}
				// if err := egp.Wait(); err != nil {
				// 	return err
				// }
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return nil, err
		}
		e.errgroupPool.Put(eg)
		stripeCnt += nextStripe
	}
	//do not forget to recover the meta replicas
	// if err := erg.Wait(); err != nil {
	// 	return nil, err
	// }
	err = e.updateDiskPath(replaceMap)
	if err != nil {
		return nil, err
	}
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}
