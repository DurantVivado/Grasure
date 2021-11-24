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
	"strconv"
	"strings"
	"sync"

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

//read the config info in config file
//Every time read file list in system warm-up
func (e *Erasure) readConfig() error {
	if ex, err := PathExist(e.configFile); !ex && err == nil {
		// we try to recover the config file from the storage system
		// the last chance to heal

		return ErrConfFileNotExist
	} else if err != nil {
		return err
	}
	// f, err := os.Open(e.configFile)
	// if err != nil {
	// 	return err
	// }
	// defer f.Close()
	// buf := bufio.NewReader(f)
	//recurrently read lines
	// _, _, err = buf.ReadLine() //dismiss the first line
	// if err != nil {
	// 	return ErrNotInitialized
	// }
	// line, _, err := buf.ReadLine()
	// if err != nil {
	// 	return ErrNotInitialized
	// }
	// split := strings.Split(string(line), " ")
	// k, err := strconv.ParseInt(split[0], 10, 32)
	// if err != nil {
	// 	return ErrNotInitialized
	// }
	// m, err := strconv.ParseInt(split[1], 10, 32)
	// if err != nil {
	// 	return ErrNotInitialized
	// }
	// line, _, err = buf.ReadLine()
	// if err != nil {
	// 	return ErrNotInitialized
	// }
	// bs, err := strconv.ParseInt(string(line), 10, 32)
	// if err != nil {
	// 	return ErrNotInitialized
	// }

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
		e.fileMap[f.FileName] = f

	}
	//e.sEnc, err = reedsolomon.NewStreamC(e.K, e.M, conReads, conWrites)
	// if err != nil {
	// 	return err
	// }
	// //next is the file lists //read all file meta
	// var str string
	// for {
	// 	//read the file name
	// 	if len(str) == 0 {
	// 		//if str is not empty, we retain previous file name
	// 		str, err = buf.ReadString('\n')
	// 	}
	// 	if err == io.EOF {
	// 		break
	// 	} else if err != nil {
	// 		return err
	// 	}
	// 	fi := &FileInfo{}
	// 	fi.FileName = strings.TrimSuffix(str, "\n")
	// 	//read the file size
	// 	str, err = buf.ReadString('\n')
	// 	if err == io.EOF {
	// 		return fmt.Errorf("%s 's meta data fileSize is incompleted, please check", fi.FileName)
	// 	} else if err != nil {
	// 		return err
	// 	}
	// 	fi.FileSize, _ = strconv.ParseInt(strings.TrimSuffix(str, "\n"), 10, 64)
	// 	//read file hash
	// 	str, err = buf.ReadString('\n')
	// 	if err != nil && err != io.EOF {
	// 		return err
	// 	}
	// 	fi.Hash = strings.TrimSuffix(str, "\n")
	// 	//read the block distribution
	// 	for {
	// 		str, err = buf.ReadString('\n')
	// 		if len(str) == 0 || str[0] != '[' {
	// 			break
	// 		}
	// 		str = strings.Trim(str, "[]\n")
	// 		var stripeDist []int
	// 		for _, s := range strings.Split(str, " ") {
	// 			num, err := strconv.Atoi(s)
	// 			if err != nil {
	// 				return err
	// 			}
	// 			stripeDist = append(stripeDist, num)
	// 		}
	// 		fi.Distribution = append(fi.Distribution, stripeDist)
	// 		if err == io.EOF {
	// 			break
	// 		} else if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	e.fileMap[fi.FileName] = fi

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
	// _, err = buf.WriteString("This file is automatically generated, DO NOT EDIT\n")
	// if err != nil {
	// 	return err
	// }
	_, err = buf.Write(data)
	if err != nil {
		return err
	}
	buf.Flush()
	// //when fileMap is changed, we update the fileList
	// for _, v := range e.fileMap {
	// 	//we use the template instead
	// 	line := fmt.Sprintf("%s\n%d\n%s\n", filepath.Base(v.fileName), v.fileSize, v.hash)
	// 	buf.WriteString(line)
	// 	for _, v := range v.distribution {
	// 		tmp := fmt.Sprintf("%v\n", v)
	// 		buf.WriteString(tmp)
	// 	}
	// }
	// buf.Flush()
	f.Sync()
	return nil
}

//reconstruct the config file if possible
func (e *Erasure) rebuildConfig() error {
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

//read META from certain file
func (e *Erasure) readMeta(fi *FileInfo) error {
	baseFileName := filepath.Base(fi.FileName)
	erg := new(errgroup.Group)
	//save the blob
	diskNum := len(e.diskInfos)
	ifs := make([]*os.File, diskNum)
	var mu sync.Mutex
	for i := range e.diskInfos {
		i := i
		//we have to make sure the dist is appended to fi.Distribution in order
		erg.Go(func() error {
			folderPath := filepath.Join(e.diskInfos[i].diskPath, baseFileName)
			// We decide the part name according to whether it belongs to data or parity
			partPath := filepath.Join(folderPath, "META")
			//Create the file and write in the parted data
			ifs[i], err = os.Open(partPath)
			if err != nil {
				return err
			}
			buf := bufio.NewReader(ifs[i])
			line, err := buf.ReadString('\n')
			if err != nil && err != io.EOF {
				return err
			}
			//read the block distribution
			line = strings.Trim(line, "[]\n")

			for _, s := range strings.Split(line, " ") {
				num, err := strconv.Atoi(s)
				if err != nil {
					return err
				}
				stripeNo := num / (e.K + e.M)
				blockNo := num % (e.K + e.M)
				mu.Lock()
				fi.Distribution[stripeNo][blockNo] = i
				mu.Unlock()
			}
			return nil
		})
	}
	if err := erg.Wait(); err != nil {
		return err
	}
	for i := range ifs {
		ifs[i].Close()
	}
	return nil
}
