package main

import (
	"io"
	"log"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
)

//get the number of files
func (e *Erasure) getFileNum() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	fileNum := 0
	e.fileMap.Range(func(key, value interface{}) bool {
		fileNum++
		return true
	})
	return fileNum

}

//recover mainly deals with a disk-level disaster reconstruction
//user should provide enough backup devices for transferring data
//the data will be restored sequentially to {recoveredDiskPath} with their
//predecessors' names.
func (e *Erasure) recover() error {
	if !e.quiet {
		log.Printf("Start recovering, totally %d files need recovery",
			e.getFileNum())
	} //first, make clear how many disks need to be recovered
	//Second, match backup partners
	//Third, concurrently recover the part of the files
	failNum := 0
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			failNum++
		}
	}
	if failNum == 0 {
		return nil
	}
	if failNum > e.DiskNum-e.K {
		return ErrTooFewDisksAlive
	}
	if failNum > len(e.diskInfos)-e.DiskNum {
		return ErrNotEnoughBackupForRecovery
	}
	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int, failNum)
	diskFailList := make(map[int]bool, failNum)
	j := e.DiskNum
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			replaceMap[i] = j
			diskFailList[i] = true
			j++
		}
	}
	//start recovering: traversing the files
	rfs := make([]*os.File, failNum) //restore fs
	ifs := make([]*os.File, e.DiskNum)
	erg := new(errgroup.Group)
	e.fileMap.Range(func(filename, fi interface{}) bool {
		basefilename := filename.(string)
		fd := fi.(*FileInfo)
		//These files can be repaired concurrently
		func() error {
			//read the current disks
			erg := e.errgroupPool.Get().(*errgroup.Group)
			defer e.errgroupPool.Put(erg)

			for i, disk := range e.diskInfos[:e.DiskNum] {
				i := i
				disk := disk
				erg.Go(func() error {
					folderPath := filepath.Join(disk.diskPath, basefilename)
					blobPath := filepath.Join(folderPath, "BLOB")
					if !disk.available {
						ifs[i] = nil
						return nil
					}
					ifs[i], err = os.Open(blobPath)
					if err != nil {
						return err
					}

					return nil
				})
			}
			if err := erg.Wait(); err != nil {
				return err
			}
			//open restore path IOs
			for i, disk := range e.diskInfos[e.DiskNum : e.DiskNum+failNum] {
				i := i
				disk := disk
				erg.Go(func() error {
					folderPath := filepath.Join(disk.diskPath, basefilename)
					blobPath := filepath.Join(folderPath, "BLOB")
					if e.override {
						if err := os.RemoveAll(folderPath); err != nil {
							return err
						}
					}
					if err := os.Mkdir(folderPath, 0666); err != nil {
						return ErrDataDirExist
					}
					rfs[i], err = os.Create(blobPath)
					if err != nil {
						return err
					}

					return nil
				})
			}
			if err := erg.Wait(); err != nil {
				return err
			}
			defer func() {
				for i := 0; i < failNum; i++ {
					if rfs[i] != nil {
						rfs[i].Close()
					}
				}
				for i := 0; i < e.DiskNum; i++ {
					if ifs[i] != nil {
						ifs[i].Close()
					}
				}
			}()
			//recover the file and write to restore path
			//we read the survival blocks
			//Since the file is striped, we have to reconstruct each stripe
			//for each stripe we rejoin the data
			stripeNum := len(fd.Distribution)
			dist := fd.Distribution
			numBlob := ceilFracInt(stripeNum, e.conStripes)
			stripeCnt := 0
			nextStripe := 0
			blobBuf := makeArr2DByte(e.conStripes, int(e.allStripeSize))
			for blob := 0; blob < numBlob; blob++ {
				if stripeCnt+e.conStripes > stripeNum {
					nextStripe = stripeNum - stripeCnt
				} else {
					nextStripe = e.conStripes
				}
				eg := e.errgroupPool.Get().(*errgroup.Group)
				for s := 0; s < nextStripe; s++ {
					s := s
					stripeNo := stripeCnt + s
					// offset := int64(subCnt) * e.allStripeSize
					eg.Go(func() error {
						erg := e.errgroupPool.Get().(*errgroup.Group)
						defer e.errgroupPool.Put(erg)
						//read all blocks in parallel
						//there are three cases of repairing
						//1. none of the failed disks contain the blocks
						//2. some of the failed disks contain the blocks
						//3. all of the failed disks contain the blocks
						for i := 0; i < e.K+e.M; i++ {
							i := i
							diskId := dist[stripeNo][i]
							disk := e.diskInfos[diskId]
							erg.Go(func() error {
								if !disk.available {
									return nil
								}
								//we also need to know the block's accurate offset with respect to disk
								offset := fd.blockToOffset[stripeNo][i]
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
						ok, err := e.enc.Verify(splitData)
						if err != nil {
							return err
						}
						if !ok {
							err = e.enc.ReconstructWithList(splitData, &diskFailList, &(fd.Distribution[stripeNo]), false)
							if err != nil {
								return err
							}
						} else {
							return nil
						}
						//write the Blob to restore paths
						egp := e.errgroupPool.Get().(*errgroup.Group)
						defer e.errgroupPool.Put(egp)
						for i := 0; i < e.K+e.M; i++ {
							i := i
							diskId := dist[stripeNo][i]
							if v, ok := replaceMap[diskId]; ok {
								restoreId := v - e.DiskNum
								writeOffset := fd.blockToOffset[stripeNo][i]
								egp.Go(func() error {
									_, err := rfs[restoreId].WriteAt(splitData[i],
										int64(writeOffset)*e.BlockSize)
									if err != nil {
										return err
									}
									return nil
								})

							}
						}
						if err := egp.Wait(); err != nil {
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
		}()
		return true
	})
	if err := erg.Wait(); err != nil {
		return err
	}
	err = e.updateDiskPath(replaceMap)
	if err != nil {
		return err
	}
	if !e.quiet {
		log.Println("Finish recovering")
	}
	return nil
}
func (e *Erasure) updateDiskPath(replaceMap map[int]int) error {
	// the last step: after recovering the files, we update `.hdr.disks.path`
	// and write a copy
	//1. rename the file
	err := os.Rename(e.diskFilePath, e.diskFilePath+".old")
	if err != nil {
		return err
	}
	//2. update e.diskFilePath
	newDiskInfos := []*DiskInfo{}
	for i := range e.diskInfos[:e.DiskNum] {
		if v, ok := replaceMap[i]; ok {
			newDiskInfos = append(newDiskInfos, e.diskInfos[v])
		} else {
			newDiskInfos = append(newDiskInfos, e.diskInfos[i])
		}
	}
	for j := e.DiskNum + len(replaceMap); j < len(e.diskInfos); j++ {
		newDiskInfos = append(newDiskInfos, e.diskInfos[j])
	}
	e.diskInfos = newDiskInfos
	//3.write to new file
	f, err := os.OpenFile(e.diskFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil
	}
	defer f.Close()
	for _, di := range e.diskInfos {
		_, err = f.WriteString(di.diskPath + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}
