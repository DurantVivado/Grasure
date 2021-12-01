package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/DurantVivado/reedsolomon"
	"golang.org/x/sync/errgroup"
)

//read the disk paths from diskFilePath
//There should be One disk path at each line
func (e *Erasure) readDiskPath() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	f, err := os.Open(e.diskFilePath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewReader(f)

	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		diskInfo := &DiskInfo{diskPath: string(line), available: true}
		e.diskInfos = append(e.diskInfos, diskInfo)
	}
	return nil
}

//initiate the erasure-coded system
func (e *Erasure) initSystem(assume bool) error {
	if !e.quiet {
		fmt.Println("Warning: you are intializing a new erasure-coded system, which means the previous data will also be reset.")
	}
	if !assume {
		if ans, err := consultUserBeforeAction(); !ans && err == nil {
			return nil
		} else if err != nil {
			return err
		}
	}
	if e.K <= 0 || e.M <= 0 {
		return reedsolomon.ErrInvShardNum
	}
	//The reedsolomon library only implements GF(2^8) and will be improved later
	if e.K+e.M > 256 {
		return reedsolomon.ErrMaxShardNum
	}
	e.DiskNum = len(e.diskInfos)
	if e.K+e.M > e.DiskNum {
		return ErrTooFewDisks
	}
	//replicate the config files
	if replicateFactor < 0 {
		return ErrNegativeReplicateFactor
	}
	e.replicateFactor = replicateFactor

	err = e.resetSystem()
	if err != nil {
		return err
	}
	if !e.quiet {
		fmt.Printf("System init!\n Erasure parameters: dataShards:%d, parityShards:%d,blocksize:%d,diskNum:%d\n",
			e.K, e.M, e.BlockSize, e.DiskNum)
	}
	return nil
}

//reset the storage assets
func (e *Erasure) reset() error {

	g := new(errgroup.Group)

	for _, path := range e.diskInfos {
		path := path
		files, err := os.ReadDir(path.diskPath)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			continue
		}
		g.Go(func() error {
			for _, file := range files {
				err = os.RemoveAll(filepath.Join(path.diskPath, file.Name()))
				if err != nil {
					return err
				}

			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

//reset the system including config and data
func (e *Erasure) resetSystem() error {

	//in-memory meta reset
	e.FileMeta = make([]*FileInfo, 0)
	// for k := range e.fileMap {
	// 	delete(e.fileMap, k)
	// }
	e.fileMap.Range(func(key, value interface{}) bool {
		e.fileMap.Delete(key)
		return true
	})
	err = e.writeConfig()
	if err != nil {
		return err
	}
	//delete the data blocks under all diskPath
	err = e.reset()
	if err != nil {
		return err
	}
	err = e.replicateConfig(e.replicateFactor)
	if err != nil {
		return err
	}
	return nil
}

//disk status
func (e *Erasure) printDiskStatus() {
	for i, disk := range e.diskInfos {

		fmt.Printf("DiskId:%d, available:%tn,numBlocks:%d, storage:%d/%d (bytes)\n",
			i, disk.available, disk.numBlocks, int64(disk.numBlocks)*e.BlockSize, disk.capacity)
	}
}

//read the config info in config file
//Every time read file list in system warm-up
func (e *Erasure) readConfig() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ex, err := PathExist(e.configFile); !ex && err == nil {
		// we try to recover the config file from the storage system
		// which renders the last chance to heal
		err = e.rebuildConfig()
		if err != nil {
			return ErrConfFileNotExist
		}
	} else if err != nil {
		return err
	}
	data, err := ioutil.ReadFile(e.configFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &e)
	if err != nil {
		//if json file is broken, we try to recover it

		err = e.rebuildConfig()
		if err != nil {
			return ErrConfFileNotExist
		}

		data, err := ioutil.ReadFile(e.configFile)
		if err != nil {
			return err
		}
		err = json.Unmarshal(data, &e)
		if err != nil {
			return err
		}
	}
	// e.K = int(k)
	// e.M = int(m)
	// e.BlockSize = blockSize
	//initialize the ReedSolomon Code
	e.enc, err = reedsolomon.New(e.K, e.M,
		reedsolomon.WithAutoGoroutines(int(e.BlockSize)),
		reedsolomon.WithCauchyMatrix(),
		reedsolomon.WithInversionCache(true),
	)
	if err != nil {
		return err
	}
	e.DiskNum = len(e.diskInfos)
	e.dataStripeSize = int64(e.K) * e.BlockSize
	e.allStripeSize = int64(e.K+e.M) * e.BlockSize

	e.errgroupPool.New = func() interface{} {
		return &errgroup.Group{}
	}
	//unzip the fileMap
	for _, f := range e.FileMeta {
		stripeNum := len(f.Distribution)
		f.blockToOffset = makeArr2DInt(stripeNum, e.K+e.M)
		countSum := make([]int, e.DiskNum)
		for row := range f.Distribution {

			for line := range f.Distribution[row] {
				diskId := f.Distribution[row][line]
				f.blockToOffset[row][line] = countSum[diskId]
				countSum[diskId]++
			}
		}
		//update the numBlocks
		for i := range countSum {
			e.diskInfos[i].numBlocks += countSum[i]
		}
		e.fileMap.Store(f.FileName, f)
		// e.fileMap[f.FileName] = f

	}
	e.FileMeta = make([]*FileInfo, 0)
	// we
	//e.sEnc, err = reedsolomon.NewStreamC(e.K, e.M, conReads, conWrites)
	// if err != nil {
	// 	return err
	// }

	return nil
}

//Replicate the config file into the system for k-fold
//it's NOT striped and encoded as a whole piece.
func (e *Erasure) replicateConfig(k int) error {
	diskNum := len(e.diskInfos)
	selectDisk := genRandomArr(diskNum, 0)[:k]
	for _, i := range selectDisk {
		disk := e.diskInfos[i]
		replicaPath := filepath.Join(disk.diskPath, "META")
		_, err = copyFile(e.configFile, replicaPath)
		if err != nil {
			log.Println(err.Error())
		}

	}
	return nil
}

//write the erasure parameters into config files
func (e *Erasure) writeConfig() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	f, err := os.OpenFile(e.configFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	// we marsh filemap into fileLists
	// for _, v := range e.fileMap {
	// 	e.FileMeta = append(e.FileMeta, v)
	// }
	e.fileMap.Range(func(k, v interface{}) bool {
		e.FileMeta = append(e.FileMeta, v.(*FileInfo))
		return true
	})
	data, err := json.Marshal(e)
	// data, err := json.MarshalIndent(e, " ", "  ")
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(f)
	_, err = buf.Write(data)
	if err != nil {
		return err
	}
	buf.Flush()
	f.Sync()

	return nil
}

//reconstruct the config file if possible
func (e *Erasure) rebuildConfig() error {
	//we read file meta in the disk path and try to rebuild the config file
	for i := range e.diskInfos {
		disk := e.diskInfos[i]
		replicaPath := filepath.Join(disk.diskPath, "META")
		if ok, err := PathExist(replicaPath); !ok && err == nil {
			continue
		}
		_, err = copyFile(replicaPath, e.configFile)
		if err != nil {
			return err
		}
		break
	}
	return nil
}

//update the config file of all replica
func (e *Erasure) updateConfigReplica() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	//we read file meta in the disk path and try to rebuild the config file
	if replicateFactor < 1 {
		return nil
	}
	for i := range e.diskInfos {
		disk := e.diskInfos[i]
		replicaPath := filepath.Join(disk.diskPath, "META")
		if ok, err := PathExist(replicaPath); !ok && err == nil {
			continue
		}
		_, err = copyFile(e.configFile, replicaPath)
		if err != nil {
			return err
		}
	}
	return nil
}

//delete specific file
func (e *Erasure) removeFile(filename string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	g := new(errgroup.Group)

	for _, path := range e.diskInfos {
		path := path
		files, err := os.ReadDir(path.diskPath)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			continue
		}
		g.Go(func() error {

			err = os.RemoveAll(filepath.Join(path.diskPath, filename))
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	e.fileMap.Delete(filename)
	// delete(e.fileMap, filename)
	log.Printf("file %s successfully deleted.", filename)
	return nil
}
