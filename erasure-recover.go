package main

import (
	"log"
	"os"
	"path/filepath"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

//recover mainly deals with a disk-level disaster reconstruction
//user should provide enough backup devices for transferring data
//the data will be restored sequentially to {recoveredDiskPath} with their
//predecessors' names.
func (e *Erasure) getFileNum() int {
	fileNum := int32(0)
	e.fileMap.Range(func(key, value interface{}) bool {
		atomic.AddInt32(&fileNum, 1)
		return true
	})
	return int(fileNum)

}
func (e *Erasure) restore() error {
	fileNum := e.getFileNum()
	//first, make clear how many disks need to be recovered
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
	backups := e.diskInfos[e.DiskNum:]
	if failNum > e.DiskNum-e.K {
		return ErrTooFewDisksAlive
	}
	if failNum > len(backups) {
		return ErrNotEnoughBackupForRecovery
	}
	if fileNum == 0 {
		//no need to restore
		return nil
	}
	failMap := make(map[int]int, failNum)
	j := 0
	for i := 0; i < e.DiskNum && j < len(backups); i++ {
		if !e.diskInfos[i].available {
			failMap[i] = j
			j++
		}
	}
	//start recovering: traversing the files
	rfs := make([]*os.File, failNum)
	erg := new(errgroup.Group)
	e.fileMap.Range(func(filename, fi interface{}) bool {
		basefilename := filename.(string)
		fi = fi.(*FileInfo)
		for i, disk := range e.diskInfos[e.DiskNum : e.DiskNum+failNum] {
			i := i
			disk := disk
			erg.Go(func() error {
				folderPath := filepath.Join(disk.diskPath, basefilename)
				blobPath := filepath.Join(folderPath, "BLOB")
				rfs[i], err = os.Open(blobPath)
				if err != nil {
					return err
				}

				return nil
			})
		}
		if err := erg.Wait(); err != nil {
			if !e.quiet {
				log.Printf("%s", err.Error())
			}
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
		return true
	})

	return nil
}
