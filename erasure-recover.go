package grasure

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

//RecoverReadFull mainly deals with a disk-level disaster reconstruction.
//User should provide enough backup devices in `.hdr.disk.path` for data transferring.
//
//An (oldPath -> replacedPath) replace map is returned in the first placeholder.
func (e *Erasure) Recover() (map[string]string, error) {
	totalFiles := e.getFileNum()
	if !e.Quiet {
		log.Printf("Start recovering, totally %d files need recovery",
			totalFiles)
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
		return nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNum > e.M {
		return nil, errTooFewDisksAlive
	}
	//the failure number doesn't exceed the fault tolerance
	//but unluckily we don't have enough backups!
	if failNum > len(e.diskInfos)-e.DiskNum {
		return nil, errNotEnoughBackupForRecovery
	}
	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	diskFailList := make(map[int]bool, failNum)
	j := e.DiskNum
	// think what if backup also breaks down, future stuff
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].diskPath] = e.diskInfos[j].diskPath
			replaceMap[i] = j
			diskFailList[i] = true
			j++
		}
	}
	//start recovering: traversing the files

	erg := new(errgroup.Group)
	// var ifpool, rfpool sync.Pool
	// ifpool.New = func() interface{} {
	// 	out := make([]*os.File, e.DiskNum)
	// 	return &out
	// }
	// rfpool.New = func() interface{} {
	// 	out := make([]*os.File, failNum)
	// 	return &out
	// }
	e.fileMap.Range(func(filename, fi interface{}) bool {
		basefilename := filename.(string)
		fd := fi.(*fileInfo)
		//These files can be repaired concurrently
		// rfs := *rfpool.Get().(*[]*os.File) //restore fs
		// ifs := *ifpool.Get().(*[]*os.File)
		// defer rfpool.Put(&rfs)
		// defer rfpool.Put(&ifs)
		ifs := make([]*os.File, e.DiskNum)
		rfs := make([]*os.File, failNum)
		erg.Go(func() error {
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
			defer func() {
				for i := 0; i < e.DiskNum; i++ {
					if ifs[i] != nil {
						ifs[i].Close()
					}
				}
			}()
			//open restore path IOs
			for i, disk := range e.diskInfos[e.DiskNum : e.DiskNum+failNum] {
				i := i
				disk := disk
				erg.Go(func() error {
					folderPath := filepath.Join(disk.diskPath, basefilename)
					blobPath := filepath.Join(folderPath, "BLOB")
					if e.Override {
						if err := os.RemoveAll(folderPath); err != nil {
							return err
						}
					}
					if err := os.Mkdir(folderPath, 0666); err != nil {
						return errDataDirExist
					}
					rfs[i], err = os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
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
			}()
			//recover the file and write to restore path
			//we read the survival blocks
			//Since the file is striped, we have to reconstruct each stripe
			//for each stripe we rejoin the data
			stripeNum := len(fd.Distribution)
			dist := fd.Distribution
			numBlob := ceilFracInt(stripeNum, e.ConStripes)
			stripeCnt := 0
			nextStripe := 0
			blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
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
							if !disk.available {
								continue
							}
							erg.Go(func() error {
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
						splitData, err := e.splitStripe(blobBuf[s])
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
									if e.diskInfos[diskId].ifMetaExist {
										newMetapath := filepath.Join(e.diskInfos[restoreId].diskPath, "META")
										if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
											return err
										}
									}
									return nil

								})

							}
						}
						if err := egp.Wait(); err != nil {
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
				log.Printf("reading %s!", filename)
			}
			return nil
		})
		return true
	})
	//do not forget to recover the meta replicas
	if err := erg.Wait(); err != nil {
		return nil, err
	}
	err = e.updateDiskPath(replaceMap)
	if err != nil {
		return nil, err
	}
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}

// Unrecommended : read k blocks every stripe
//Recover mainly deals with a disk-level disaster reconstruction.
//User should provide enough backup devices in `.hdr.disk.path` for data transferring.
//
//An (oldPath -> replacedPath) replace map is returned in the first placeholder.
func (e *Erasure) RecoverReadK() (map[string]string, error) {
	totalFiles := e.getFileNum()
	if !e.Quiet {
		log.Printf("Start recovering, totally %d files need recovery",
			totalFiles)
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
		return nil, nil
	}
	//the failure number exceeds the fault tolerance
	if failNum > e.M {
		return nil, errTooFewDisksAlive
	}
	//the failure number doesn't exceed the fault tolerance
	//but unluckily we don't have enough backups!
	if failNum > len(e.diskInfos)-e.DiskNum {
		return nil, errNotEnoughBackupForRecovery
	}
	//the failed disks are mapped to backup disks
	replaceMap := make(map[int]int)
	ReplaceMap := make(map[string]string)
	diskFailList := make(map[int]bool, failNum)
	j := e.DiskNum
	// think what if backup also breaks down, future stuff
	for i := 0; i < e.DiskNum; i++ {
		if !e.diskInfos[i].available {
			ReplaceMap[e.diskInfos[i].diskPath] = e.diskInfos[j].diskPath
			replaceMap[i] = j
			diskFailList[i] = true
			j++
		}
	}
	//start recovering: traversing the files

	erg := new(errgroup.Group)
	// var ifpool, rfpool sync.Pool
	// ifpool.New = func() interface{} {
	// 	out := make([]*os.File, e.DiskNum)
	// 	return &out
	// }
	// rfpool.New = func() interface{} {
	// 	out := make([]*os.File, failNum)
	// 	return &out
	// }
	e.fileMap.Range(func(filename, fi interface{}) bool {
		basefilename := filename.(string)
		fd := fi.(*fileInfo)
		//These files can be repaired concurrently
		// rfs := *rfpool.Get().(*[]*os.File) //restore fs
		// ifs := *ifpool.Get().(*[]*os.File)
		// defer rfpool.Put(&rfs)
		// defer rfpool.Put(&ifs)
		ifs := make([]*os.File, e.DiskNum)
		rfs := make([]*os.File, failNum)
		erg.Go(func() error {
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
			defer func() {
				for i := 0; i < e.DiskNum; i++ {
					if ifs[i] != nil {
						ifs[i].Close()
					}
				}
			}()
			//open restore path IOs
			for i, disk := range e.diskInfos[e.DiskNum : e.DiskNum+failNum] {
				i := i
				disk := disk
				erg.Go(func() error {
					folderPath := filepath.Join(disk.diskPath, basefilename)
					blobPath := filepath.Join(folderPath, "BLOB")
					if e.Override {
						if err := os.RemoveAll(folderPath); err != nil {
							return err
						}
					}
					if err := os.Mkdir(folderPath, 0666); err != nil {
						return errDataDirExist
					}
					rfs[i], err = os.OpenFile(blobPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
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
			}()
			//recover the file and write to restore path
			//we read the survival blocks
			//Since the file is striped, we have to reconstruct each stripe
			//for each stripe we rejoin the data
			stripeNum := len(fd.Distribution)
			dist := fd.Distribution
			numBlob := ceilFracInt(stripeNum, e.ConStripes)
			stripeCnt := 0
			nextStripe := 0
			blobBuf := makeArr2DByte(e.ConStripes, int(e.allStripeSize))
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
					// offset := int64(subCnt) * e.allStripeSize
					eg.Go(func() error {
						erg := e.errgroupPool.Get().(*errgroup.Group)
						defer e.errgroupPool.Put(erg)
						//read all blocks in parallel
						//there are three cases of repairing
						//1. none of the failed disks contain the blocks
						//2. some of the failed disks contain the blocks
						//3. all of the failed disks contain the blocks
						cntAlive := []int{}
						cntFail := []int{}
						for i := 0; i < e.K+e.M; i++ {
							i := i
							diskId := dist[stripeNo][i]
							disk := e.diskInfos[diskId]
							if !disk.available {
								cntFail = append(cntFail, i)
								continue
							}
							if len(cntAlive) < e.K {
								cntAlive = append(cntAlive, i)

								erg.Go(func() error {
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
							// err = e.enc.ReconstructWithList(splitData, &diskFailList, &(fd.Distribution[stripeNo]), false)
							err = e.enc.ReconstructWithKBlocks(splitData, cntAlive, cntFail, false)
							if err != nil {
								return err
							}
						} else {
							//there is not broken block on this stripe
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
									if e.diskInfos[diskId].ifMetaExist {
										newMetapath := filepath.Join(e.diskInfos[restoreId].diskPath, "META")
										if _, err := copyFile(e.ConfigFile, newMetapath); err != nil {
											return err
										}
									}
									return nil

								})

							}
						}
						if err := egp.Wait(); err != nil {
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
				log.Printf("reading %s!", filename)
			}
			return nil
		})
		return true
	})
	//do not forget to recover the meta replicas
	if err := erg.Wait(); err != nil {
		return nil, err
	}
	err = e.updateDiskPath(replaceMap)
	if err != nil {
		return nil, err
	}
	if !e.Quiet {
		log.Println("Finish recovering")
	}
	return ReplaceMap, nil
}

//Update the diskpath. Reserve the current diskPathFile and write new one.
func (e *Erasure) updateDiskPath(replaceMap map[int]int) error {
	// the last step: after recovering the files, we update `.hdr.disks.path`
	// and write a copy
	//1. rename the file
	err := os.Rename(e.DiskFilePath, e.DiskFilePath+".old")
	if err != nil {
		return err
	}
	//2. update e.DiskFilePath
	for k, v := range replaceMap {
		e.diskInfos[k] = e.diskInfos[v]
	}
	// A little trick on removal of center elements
	fn := len(replaceMap)
	e.diskInfos = e.diskInfos[:e.DiskNum+
		copy(e.diskInfos[e.DiskNum:], e.diskInfos[e.DiskNum+fn:])]
	//3.write to new file
	f, err := os.OpenFile(e.DiskFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
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
