package grasure

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

// ReadDiskPath reads the disk paths from diskFilePath.
// There should be exactly ONE disk path at each line.
//
// This func can NOT be called concurrently.
func (e *Erasure) ReadDiskPath() error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	f, err := os.Open(e.DiskFilePath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := bufio.NewReader(f)
	e.diskInfos = make([]*diskInfo, 0)
	for {
		line, _, err := buf.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		path := string(line)
		if ok, err := pathExist(path); !ok && err == nil {
			return &diskError{path, "disk path not exist"}
		} else if err != nil {
			return err
		}
		metaPath := filepath.Join(path, "META")
		flag := false
		if ok, err := pathExist(metaPath); ok && err == nil {
			flag = true
		} else if err != nil {
			return err
		}
		diskInfo := &diskInfo{diskPath: string(line), available: true, ifMetaExist: flag}
		e.diskInfos = append(e.diskInfos, diskInfo)
	}
	return nil
}

// Init initiates the erasure-coded system, this func can NOT be called concurrently.
// It will clear all the data on the storage, so a consulting procedure is added in advance of perilous action.
//
// Note if `assume` renders yes then the consulting part will be skipped.
func (e *Erasure) InitSystem(assume bool) error {
	if !e.Quiet {
		fmt.Println("Warning: you are initializing a new erasure-coded system, which means the previous data will also be reset.")
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
	if e.K+e.M > e.DiskNum {
		return errTooFewDisksAlive
	}
	if e.DiskNum > len(e.diskInfos) {
		return errDiskNumTooLarge
	}
	if e.ConStripes < 1 {
		e.ConStripes = 1 //totally serialized
	}
	//replicate the config files

	if e.ReplicateFactor < 1 {
		return errInvalidReplicateFactor
	}
	err = e.resetSystem()
	if err != nil {
		return err
	}
	if !e.Quiet {
		log.Printf("System initialized!\nSystem parameters: \n")
		fmt.Printf(" %-20s:%20d\n", "data shards", e.K)
		fmt.Printf(" %-20s:%20d\n", "parity shards", e.M)
		fmt.Printf(" %-20s:%20d\n", "block size (bytes)", e.BlockSize)
		fmt.Printf(" %-20s:%20d\n", "used disk number", e.DiskNum)
		fmt.Printf(" %-20s:%20d\n", "total disk number",
			len(e.diskInfos))
		fmt.Printf(" %-20s:%20d\n", "memory limit (GiB)", e.MemSize)

	}
	return nil
}

// reset the storage assets
func (e *Erasure) reset() error {

	g := new(errgroup.Group)

	for _, path := range e.diskInfos[:e.DiskNum] {
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

// reset the system including config and data
func (e *Erasure) resetSystem() error {

	//in-memory meta reset
	e.FileMeta = make([]*fileInfo, 0)
	e.StripeInDisk = make([][]int64, len(e.diskInfos))
	// for k := range e.fileMap {
	// 	delete(e.fileMap, k)
	// }
	e.fileMap.Range(func(key, value interface{}) bool {
		e.fileMap.Delete(key)
		return true
	})
	err = e.WriteConfig()
	if err != nil {
		return err
	}
	//delete the data blocks under all diskPath
	err = e.reset()
	if err != nil {
		return err
	}
	err = e.replicateConfig(e.ReplicateFactor)
	if err != nil {
		return err
	}
	return nil
}

// ReadConfig reads the config file during system warm-up.
//
// Calling it before actions like encode and read is a good habit.
func (e *Erasure) ReadConfig() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if ex, err := pathExist(e.ConfigFile); !ex && err == nil {
		// we try to recover the config file from the storage system
		// which renders the last chance to heal
		err = e.rebuildConfig()
		if err != nil {
			return errConfFileNotExist
		}
	} else if err != nil {
		return err
	}
	data, err := ioutil.ReadFile(e.ConfigFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &e)
	if err != nil {
		//if json file is broken, we try to recover it

		err = e.rebuildConfig()
		if err != nil {
			return errConfFileNotExist
		}

		data, err := ioutil.ReadFile(e.ConfigFile)
		if err != nil {
			return err
		}
		err = json.Unmarshal(data, &e)
		if err != nil {
			return err
		}
	}
	//initialize the ReedSolomon Code
	e.enc, err = reedsolomon.New(e.K, e.M,
		reedsolomon.WithAutoGoroutines(int(e.BlockSize)),
		reedsolomon.WithCauchyMatrix(),
		reedsolomon.WithInversionCache(true),
	)
	if err != nil {
		return err
	}
	e.dataStripeSize = int64(e.K) * e.BlockSize
	e.allStripeSize = int64(e.K+e.M) * e.BlockSize

	e.errgroupPool.New = func() interface{} {
		return &errgroup.Group{}
	}
	//unzip the fileMap
	for _, f := range e.FileMeta {
		stripeNum := len(f.Distribution)
		f.blockToOffset = makeArr2DInt(stripeNum, e.K+e.M)
		f.blockInfos = make([][]*blockInfo, stripeNum)
		countSum := make([]int, e.DiskNum)
		for row := range f.Distribution {
			f.blockInfos[row] = make([]*blockInfo, e.K+e.M)
			for line := range f.Distribution[row] {
				diskId := f.Distribution[row][line]
				f.blockToOffset[row][line] = countSum[diskId]
				f.blockInfos[row][line] = &blockInfo{bstat: blkOK}
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
	e.FileMeta = make([]*fileInfo, 0)
	// we
	//e.sEnc, err = reedsolomon.NewStreamC(e.K, e.M, conReads, conWrites)
	// if err != nil {
	// 	return err
	// }

	return nil
}

// Replicate the config file into the system for k-fold
// it's NOT striped and encoded as a whole piece.
func (e *Erasure) replicateConfig(k int) error {
	selectDisk := genRandomArr(e.DiskNum, 0)[:k]
	for _, i := range selectDisk {
		disk := e.diskInfos[i]
		disk.ifMetaExist = true
		replicaPath := filepath.Join(disk.diskPath, "META")
		_, err = copyFile(e.ConfigFile, replicaPath)
		if err != nil {
			log.Println(err.Error())
		}

	}
	return nil
}

// WriteConfig writes the erasure parameters and file information list into config files.
//
// Calling it after actions like encode and read is a good habit.
func (e *Erasure) WriteConfig() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	f, err := os.OpenFile(e.ConfigFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	// we marsh filemap into fileLists
	// for _, v := range e.fileMap {
	// 	e.FileMeta = append(e.FileMeta, v)
	// }
	e.fileMap.Range(func(k, v interface{}) bool {
		e.FileMeta = append(e.FileMeta, v.(*fileInfo))
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
	// f.Sync()
	err = e.updateConfigReplica()
	if err != nil {
		return err
	}
	return nil
}

// reconstruct the config file if possible
func (e *Erasure) rebuildConfig() error {
	//we read file meta in the disk path and try to rebuild the config file
	for i := range e.diskInfos[:e.DiskNum] {
		disk := e.diskInfos[i]
		replicaPath := filepath.Join(disk.diskPath, "META")
		if ok, err := pathExist(replicaPath); !ok && err == nil {
			continue
		}
		_, err = copyFile(replicaPath, e.ConfigFile)
		if err != nil {
			return err
		}
		break
	}
	return nil
}

// update the config file of all replica
func (e *Erasure) updateConfigReplica() error {

	//we read file meta in the disk path and try to rebuild the config file
	if e.ReplicateFactor < 1 {
		return nil
	}
	for i := range e.diskInfos[:e.DiskNum] {
		disk := e.diskInfos[i]
		replicaPath := filepath.Join(disk.diskPath, "META")
		if ok, err := pathExist(replicaPath); !ok && err == nil {
			continue
		}
		_, err = copyFile(e.ConfigFile, replicaPath)
		if err != nil {
			return err
		}
	}
	return nil
}

// RemoveFile deletes specific file `filename`in the system.
//
// Both the file blobs and meta data are deleted. It's currently irreversible.
func (e *Erasure) RemoveFile(filename string) error {
	baseFilename := filepath.Base(filename)
	if _, ok := e.fileMap.Load(baseFilename); !ok {
		return fmt.Errorf("the file %s does not exist in the file system",
			baseFilename)
	}
	g := new(errgroup.Group)

	for _, path := range e.diskInfos[:e.DiskNum] {
		path := path
		files, err := os.ReadDir(path.diskPath)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			continue
		}
		g.Go(func() error {

			err = os.RemoveAll(filepath.Join(path.diskPath, baseFilename))
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	e.fileMap.Delete(baseFilename)
	// delete(e.fileMap, filename)
	if !e.Quiet {
		log.Printf("file %s successfully deleted.", baseFilename)
	}
	return nil
}

// check if file exists both in config and storage blobs
func (e *Erasure) checkIfFileExist(filename string) (bool, error) {
	//1. first check the storage blobs if file still exists
	baseFilename := filepath.Base(filename)

	g := new(errgroup.Group)

	for _, path := range e.diskInfos[:e.DiskNum] {
		path := path
		files, err := os.ReadDir(path.diskPath)
		if err != nil {
			return false, err
		}
		if len(files) == 0 {
			continue
		}
		g.Go(func() error {

			subpath := filepath.Join(path.diskPath, baseFilename)
			if ok, err := pathExist(subpath); !ok && err == nil {
				return errFileBlobNotFound
			} else if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return false, err
	}
	//2. check if fileMap contains the file
	if _, ok := e.fileMap.Load(baseFilename); !ok {
		return false, nil
	}
	return true, nil
}
