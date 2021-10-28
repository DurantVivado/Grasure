package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/klauspost/reedsolomon"
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
func (e *Erasure) init() {
	fmt.Println("Warning: you are intializing a new erasure-coded system, which means the previous data will also be reset.\n Are you sure to proceed?[Y]es, or [N]o:")
	if ans, err := consultUserBeforeAction(); !ans {
		return
	} else if err != nil {
		failOnErr(*mode, err)
	}
	e.k = *k
	e.m = *m
	if e.k <= 0 || e.m <= 0 {
		failOnErr(*mode, reedsolomon.ErrInvShardNum)
	}
	//The reedsolomon library only implements GF(2^8) and will be improved later
	if e.k+e.m > 256 {
		failOnErr(*mode, reedsolomon.ErrMaxShardNum)
	}
	e.blockSize = *blockSize
	err = e.readDiskPath()
	failOnErr(*mode, err)
	if e.k+e.m > len(e.diskInfos) {
		failOnErr(*mode, ErrTooFewDisks)
	}
	//we persit meta info info in hard drives
	err = e.writeConfig()
	failOnErr(*mode, err)
	//delete the data blocks under all diskPath
	err = e.reset()
	failOnErr(*mode, err)
	fmt.Println("System init!")
}

//read the config info in config file
//Every time read file list in system warm-up
func (e *Erasure) readConfig() error {
	if ex, err := PathExist(e.configFile); !ex && err == nil {
		return ErrConfFileNotExist
	} else if err != nil {
		return err
	}
	f, err := os.Open(e.configFile)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewReader(f)
	//recurrently read lines
	_, _, err = buf.ReadLine() //dismiss the first line
	if err != nil {
		return ErrNotInitialized
	}
	line, _, err := buf.ReadLine()
	if err != nil {
		return ErrNotInitialized
	}
	split := strings.Split(string(line), " ")
	k, err := strconv.ParseInt(split[0], 10, 32)
	if err != nil {
		return ErrNotInitialized
	}
	m, err := strconv.ParseInt(split[1], 10, 32)
	if err != nil {
		return ErrNotInitialized
	}
	line, _, err = buf.ReadLine()
	if err != nil {
		return ErrNotInitialized
	}
	bs, err := strconv.ParseInt(string(line), 10, 32)
	if err != nil {
		return ErrNotInitialized
	}
	e.k = int(k)
	e.m = int(m)
	e.blockSize = int(bs)
	//initialize the ReedSolomon Code
	e.enc, err = reedsolomon.New(e.k, e.m)
	if err != nil {
		return err
	}
	//next is the file lists //read all file meta
	for {
		line, err := buf.ReadString('\n')
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		fi := &FileInfo{}
		fi.fileName = strings.TrimSuffix(line, "\n")
		line, err = buf.ReadString('\n')
		if err == io.EOF {
			return fmt.Errorf("%s 's meta data fileSize is incompleted, please check", fi.fileName)
		} else if err != nil {
			return err
		}
		fi.fileSize, _ = strconv.ParseInt(strings.TrimSuffix(line, "\n"), 10, 64)
		//read next line
		line, err = buf.ReadString('\n')
		if err == io.EOF {
			return fmt.Errorf("%s 's meta data hash is incompleted, please check", fi.fileName)
		} else if err != nil {
			return err
		}
		fi.hash = strings.TrimSuffix(line, "\n")
		line, err = buf.ReadString('\n')
		if err == io.EOF {
			return fmt.Errorf("%s 's meta data distribution is incompleted, please check", fi.fileName)
		} else if err != nil {
			return err
		}
		line = strings.Trim(line, "[]\n")
		for _, s := range strings.Split(line, " ") {
			num, err := strconv.Atoi(s)
			if err != nil {
				return err
			}
			fi.distribution = append(fi.distribution, num)
		}
		e.fileMap[fi.fileName] = fi

	}
	return nil
}

//write the erasure parameters into config files
func (e *Erasure) writeConfig() error {

	f, err := os.OpenFile(e.configFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0754)
	if err != nil {
		return err
	}
	defer f.Close()

	//1. k,m
	//2. rand seed
	buf := bufio.NewWriter(f)
	_, err = buf.WriteString("This file is automatically generated, DO NOT EDIT\n")
	if err != nil {
		return err
	}
	line := fmt.Sprintf("%d %d\n%d\n", e.k, e.m, e.blockSize)
	_, err = buf.WriteString(line)
	if err != nil {
		return err
	}
	//when fileMap is changed, we update the fileList
	for _, v := range e.fileMap {
		line := fmt.Sprintf("%s\n%d\n%s\n%v\n", v.fileName, v.fileSize, v.hash, v.distribution)
		buf.WriteString(line)
	}
	buf.Flush()
	f.Sync()
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
				err = os.RemoveAll(path.diskPath + "/" + file.Name())
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
			err = os.RemoveAll(path.diskPath + "/" + filename)
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
	return nil
}