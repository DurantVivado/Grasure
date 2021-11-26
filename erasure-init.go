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
func (e *Erasure) initHDR() {
	fmt.Println("Warning: you are intializing a new erasure-coded system, which means the previous data will also be reset.")
	if ans, err := consultUserBeforeAction(); !ans {
		return
	} else if err != nil {
		failOnErr(mode, err)
	}
	e.K = k
	e.M = m
	if e.K <= 0 || e.M <= 0 {
		failOnErr(mode, reedsolomon.ErrInvShardNum)
	}
	//The reedsolomon library only implements GF(2^8) and will be improved later
	if e.K+e.M > 256 {
		failOnErr(mode, reedsolomon.ErrMaxShardNum)
	}
	e.BlockSize = blockSize
	err = e.readDiskPath()
	failOnErr(mode, err)
	if e.K+e.M > len(e.diskInfos) {
		failOnErr(mode, ErrTooFewDisks)
	}
	//we persit meta info info in hard drives
	err = e.writeConfig()
	failOnErr(mode, err)
	//delete the data blocks under all diskPath
	err = e.reset()
	failOnErr(mode, err)
	fmt.Printf("System init!\n Erasure parameters: dataShards:%d, parityShards:%d,blocksize:%d\n",
		k, m, blockSize)
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
	if ex, err := PathExist(e.configFile); !ex && err == nil {
		// we try to recover the config file from the storage system
		// which renders the last chance to heal
		err = e.rebuildConfig(e.configFile)
		if err != nil {
			return err
		}
		return ErrConfFileNotExist
	} else if err != nil {
		return err
	}
	err = erasure.readDiskPath()
	if err != nil {
		return fmt.Errorf("readDiskPath:%s error:%s", e.diskFilePath, err.Error())
	}
	data, err := ioutil.ReadFile(e.configFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, &e)
	if err != nil {
		return err
	}
	// e.K = int(k)
	// e.M = int(m)
	// e.BlockSize = bs
	e.conStripes = conStripes
	//initialize the ReedSolomon Code
	e.enc, err = reedsolomon.New(e.K, e.M,
		reedsolomon.WithAutoGoroutines(int(e.BlockSize)),
		reedsolomon.WithCauchyMatrix(),
		reedsolomon.WithConcurrentStreams(true),
		reedsolomon.WithInversionCache(true),
		reedsolomon.WithFastOneParityMatrix(),
	)
	if err != nil {
		return err
	}
	e.dataStripeSize = int64(e.K) * blockSize
	e.allStripeSize = int64(e.K+e.M) * blockSize

	e.errgroupPool.New = func() interface{} {
		return &errgroup.Group{}
	}
	//unzip the fileMap
	for _, f := range e.FileMeta {
		stripeNum := len(f.Distribution)
		f.blockToOffset = makeArr2DInt(stripeNum, e.K+e.M)
		countSum := make([]int, len(e.diskInfos))
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
		e.fileMap[f.FileName] = f

	}
	// we
	//e.sEnc, err = reedsolomon.NewStreamC(e.K, e.M, conReads, conWrites)
	// if err != nil {
	// 	return err
	// }

	return nil
}

//write the erasure parameters into config files
func (e *Erasure) writeConfig() error {

	f, err := os.OpenFile(e.configFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	if err != nil {
		return err
	}
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
func (e *Erasure) rebuildConfig(restorePath string) error {
	//we read file meta in the disk path and try to rebuild the config file
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

//delete specific file
func (e *Erasure) removeFile(filename string) error {
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
	delete(e.fileMap, filename)
	log.Printf("file %s successfully deleted.", filename)
	return nil
}
