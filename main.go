/*
 * @Author: your name
 * @Date: 2021-09-06 04:47:11
 * @LastEditTime: 2021-09-06 04:47:11
 * @LastEditors: Please set LastEditors
 * @Description: We decide to consider every operation in concurrent manner
 * @ProjectUrl: github.com/DurantVivado/Grasure
 */
package main

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"golang.org/x/sync/errgroup"
)

type CustomerAPI interface {
	Read(filename string) ([]byte, error)
	Write(filename string) (bool, error)
	ReadAll(filename []string) ([][]byte, error)
	WriteAll(filename []string) (bool, error)
	Delete(filename string) (bool, error)
	Change(filename string) (bool, error) //change file's meta
}
type HDRInfo struct {
	used  uint64
	free  uint64
	total uint64
}

type DiskInfo struct {
	diskPath  string
	available bool
}
type Erasure struct {
	k          int                  // the number of data blocks in a stripe
	m          int                  // the number of parity blocks in a stripe
	enc        reedsolomon.Encoder  // the reedsolomon encoder
	diskInfos  []*DiskInfo          //disk paths
	configFile string               //configure file
	fileMap    map[string]*FileInfo //File info lists
	rwmu       sync.RWMutex         //read write mutex
}
type FileInfo struct {
	fileName string //file name
	fileSize int64  //file size
	// metaInfo     *os.FileInfo //system-level file info
	hash         string //hash value (SHA256 by default)
	distribution []int  //distribution represents the block replacement respect to disks
}

//the parameter lists
var mode = flag.String("mode", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")
var k = flag.Int("k", 12, "the number of data shards(<256)")
var m = flag.Int("m", 4, "the number of parity shards(2-4)")
var diskPath = flag.String("diskPath", "", "the disks path")
var file = flag.String("file", "", "the file path")
var savePath = flag.String("savePath", "file.save", "the local saving path for file")
var new_k = flag.Int("new_k", 16, "the new number of data shards(<256)")
var new_m = flag.Int("new_m", 4, "the new number of parity shards(2-4)")
var recoveredDiskPath = flag.String("recoveredDiskPath", "/tmp/data", "the data path for recovered disk, default to /tmp/data")
var failMode = flag.String("failMode", "diskFail", "simulate diskFail or bitRot mode")
var failNum = flag.Int("failNum", 0, "simulate multiple disk failure, provides the fail number of disks")
var override = flag.Bool("override", false, "whether to override former files or directories")

//Error definitions
var ErrConfFileNotExist = errors.New("the conf file not exist")
var ErrEmptyData = errors.New("the file to encode is empty")
var ErrDataDirExist = errors.New("data directory already exists")
var ErrTooFewDisks = errors.New("too few disks, i.e., k+m < N")
var ErrNotInitialized = errors.New("system not initialized, please initialize with `-mode init` first")
var ErrFileNotFound = errors.New("file not found")
var ErrSurvivalNotEnoughForDecoding = errors.New("the failed block number exceeds fault tolerance, data renders unrecoverable")
var ErrFileIncompleted = errors.New("file hash check fails, file renders incompleted")
var ErrFailModeNotRecognized = errors.New("the fail mode is not recognizable, please specify in \"diskFail\" or \"bitRot\"")

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
	e.k = int(k)
	e.m = int(m)
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

func PathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

//read the disk paths from diskPathFile
//There should be One disk path at each line
func (e *Erasure) readDiskPath(filename string) error {
	f, err := os.Open(filename)
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
	line := fmt.Sprintf("%d %d\n", e.k, e.m)
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

var err error

//consult user to avoid maloperation
func consultUserBeforeAction() (bool, error) {
	log.Println("If you are sure to proceed, type: [Y]es otherwise [N]o.")

	inputReader := bufio.NewReader(os.Stdin)
	for {
		ans, err := inputReader.ReadString('\n')
		if err != nil {
			return false, err
		}
		ans = strings.TrimSuffix(ans, "\n")
		if ans == "Y" || ans == "y" || ans == "Yes" || ans == "yes" {
			return true, nil
		} else if ans == "N" || ans == "n" || ans == "No" || ans == "no" {
			return false, nil
		} else {
			fmt.Println("Please do not make joke")
		}
	}

}
func main() {
	//We read each file and make byte flow
	flag.Parse()
	//We read the config file
	erasure := new(Erasure)
	erasure.configFile = ".hdr.sys"
	erasure.fileMap = make(map[string]*FileInfo)
	diskPathFile := ".hdr.disks.path"
	failOnErr := func(mode string, e error) {
		if e != nil {
			log.Fatalf("%s: %s", mode, e.Error())
		}
	}
	switch *mode {
	case "init":
		//initiate the erasure-coded system
		fmt.Println("Warning: you are intializing a new erasure-coded system, which means the previous data will also be reset.\n Are you sure to proceed?[Y]es, or [N]o:")
		if ans, err := consultUserBeforeAction(); !ans {
			return
		} else if err != nil {
			failOnErr(*mode, err)
		}
		erasure.k = *k
		erasure.m = *m
		err = erasure.readDiskPath(diskPathFile)
		failOnErr(*mode, err)
		if erasure.k+erasure.m > len(erasure.diskInfos) {
			failOnErr(*mode, ErrTooFewDisks)
		}
		//we persit meta info info in hard drives
		err = erasure.writeConfig()
		failOnErr(*mode, err)
		//delete the data blocks under all diskPath
		err = erasure.reset()
		failOnErr(*mode, err)
		fmt.Println("System init!")
	case "read":
		//read a file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath(diskPathFile)
		failOnErr(*mode, err)
		erasure.destroy(*failMode, *failNum)
		err = erasure.read(*file, *savePath)
		failOnErr(*mode, err)
	case "encode":
		//We are entering the encoding mode, and for brevity,we only encode one file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath(diskPathFile)
		failOnErr(*mode, err)
		_, err := erasure.encode(*file)
		failOnErr(*mode, err)
		err = erasure.writeConfig()
		failOnErr(*mode, err)
	case "update":
		//update an old file according to a new file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath(diskPathFile)
		failOnErr(*mode, err)
		err = erasure.update(*file)
		failOnErr(*mode, err)

	// case "scaling":
	// 	//scaling the system, ALERT: this is a system-level operation and irreversible
	// 	e.readConfig()
	// 	scaling(new_k, new_m)
	// case "recover":
	// 	//recover all the blocks of a disk and put the recovered result to new path
	// 	e.readConfig()
	// 	recover(*recoveredDiskPath)
	//case "delete":

	default:
		log.Fatalf("Can't parse the parameters, please check %s!", *mode)
	}
	//It functions as a testbed, so currently I won't use goroutines.

}

//split and encode a file into parity blocks concurrently
func (e *Erasure) encode(filename string) (*FileInfo, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	//We sum the hash of the file
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	hashStr := fmt.Sprintf("%x", h.Sum(nil))
	f.Seek(0, 0)
	fi := &FileInfo{}
	fi.hash = hashStr
	fi.fileName = filename
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	//we allocate the buffer actually the same size of the file
	// fmt.Println(fileInfo)
	size := fileInfo.Size()
	fi.fileSize = size
	data := make([]byte, size)
	buf := bufio.NewReader(f)
	_, err = buf.Read(data)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, ErrEmptyData
	}
	//we split the data, and create empty parity
	shards, err := e.enc.Split(data)
	if err != nil {
		return nil, err
	}
	// test, err := os.OpenFile("testJoin.txt", os.O_CREATE|os.O_RDWR, 0666)
	// if err != nil {
	// 	return nil, err
	// }
	// defer test.Close()
	// err = e.enc.Join(test, shards, int(size))
	// if err != nil {
	// 	return nil, err
	// }
	// if _, err := io.Copy(h, test); err != nil {
	// 	return nil, err
	// }
	// hashTest := fmt.Sprintf("%x", h.Sum(nil))
	// if strings.Compare(hashStr, hashTest) == 0 {
	// 	fmt.Println("ok")
	// }
	//encode the data
	err = e.enc.Encode(shards)
	if err != nil {
		return nil, err
	}
	//verify the data
	ok, err := e.enc.Verify(shards)
	if !ok || err != nil {
		return nil, err
	}
	//we save the encoded shards to dst
	//Before tht, we shuffle the data and paritys
	numDisks := len(e.diskInfos)
	shuff := make([]int, numDisks)
	for i := 0; i < numDisks; i++ {
		shuff[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
	fi.distribution = shuff
	e.fileMap[filename] = fi
	//we save the data and parity to the mounted data point of each disk
	//we adopt the methods of Minio, which first create a folder with the same name as the file,
	//and store each part of the file in the folder with name like "D_X" or "P_X".
	//Remember these files are seperable, use byte slices to ignite your desiderata
	//create folder with name of the file
	var folderPath, partPath string
	for i, path := range e.diskInfos {
		folderPath = path.diskPath + "/" + filename
		//first we create a folder
		if *override {

			if err := os.RemoveAll(folderPath); err != nil {
				return nil, err
			}

		}
		if err := os.Mkdir(folderPath, 0666); err != nil {
			return nil, ErrDataDirExist
		}
		//We decide the part name according to whether it belongs to data or parity
		if shuff[i] < e.k { //data block
			partPath = folderPath + "/D_" + fmt.Sprintf("%d", shuff[i])
		} else {
			partPath = folderPath + "/P_" + fmt.Sprintf("%d", shuff[i])

		}
		//Create the file and write in the parted data
		nf, err := os.OpenFile(partPath, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		defer nf.Close()
		buf := bufio.NewWriter(nf)
		_, err = buf.Write(shards[shuff[i]])
		if err != nil {
			return nil, err
		}
		buf.Flush()

	}

	return fi, nil
}

//read file on the system and return byte stream, include recovering
func (e *Erasure) read(filename string, savepath string) error {
	//1. we find if it is recorded on the conf
	fi, ok := e.fileMap[filename]
	if !ok {
		return ErrFileNotFound
	}
	g := new(errgroup.Group)
	survivalParity := []int{}
	survivalData := []int{}
	// var mu sync.Mutex
	//first we check whether the remaining parts are enough for decoding
	for i, path := range e.diskInfos {
		basePath := path.diskPath
		i := i
		if !path.available {
			continue
		}
		g.Go(func() error {
			var fullPath string
			if fi.distribution[i] < e.k {
				fullPath = basePath + "/" + filename + "/D_" + fmt.Sprintf("%d", fi.distribution[i])
			} else {

				fullPath = basePath + "/" + filename + "/P_" + fmt.Sprintf("%d", fi.distribution[i])
			}
			ex, err := PathExist(fullPath)
			if err != nil {
				return fmt.Errorf("%s fail with error: %s", fullPath, err.Error())
			}
			if ex {
				if fi.distribution[i] < e.k {
					// log.Println(fullPath)
					survivalData = append(survivalData, i)
				} else {
					// log.Println(fullPath)
					survivalParity = append(survivalParity, i)
				}
			}
			return nil
		})

	}
	if err := g.Wait(); err != nil {
		return err
	}

	if len(survivalData)+len(survivalParity) < e.k {
		return ErrSurvivalNotEnoughForDecoding
	}
	//We need to decode the file using parity
	totalBytes := make([][]byte, e.k+e.m)
	survivalData = append(survivalData, survivalParity...)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(survivalData), func(i, j int) { survivalData[i], survivalData[j] = survivalData[j], survivalData[i] })
	//for minimizing read overhead, we only choose first k blocks
	for _, ind := range survivalData[:e.k] {
		ind := ind
		g.Go(func() error {
			var fullpath string
			if fi.distribution[ind] < e.k {
				fullpath = e.diskInfos[ind].diskPath + "/" + filename + "/D_" + fmt.Sprintf("%d", fi.distribution[ind])
			} else {
				fullpath = e.diskInfos[ind].diskPath + "/" + filename + "/P_" + fmt.Sprintf("%d", fi.distribution[ind])

			}

			f, err := os.Open(fullpath)
			if err != nil {
				return err
			}
			defer f.Close()
			blockByte := (int(fi.fileSize) + e.k - 1) / e.k
			totalBytes[fi.distribution[ind]] = make([]byte, blockByte)
			_, err = f.Read(totalBytes[fi.distribution[ind]])
			if err != nil {
				return err
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	//reconstructing, we first decode data, once completed
	//notify the customer, and parity reconstruction, we move it to back-end
	err = e.enc.Reconstruct(totalBytes)
	if err != nil {
		return err
	}

	f, err := os.OpenFile(savepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	err = e.enc.Join(f, totalBytes, int(fi.fileSize))
	if err != nil {
		return err
	}
	//checksum
	f.Seek(0, 0)
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil
	}
	hashSum := fmt.Sprintf("%x", h.Sum(nil))
	if strings.Compare(hashSum, fi.hash) != 0 {
		return ErrFileIncompleted
	}
	log.Printf("%s successfully read (Decoded)!", filename)
	return nil
}

//simulate disk failure or bitrot
func (e *Erasure) destroy(mode string, failNum int) {
	if mode == "diskFail" {
		if failNum == 0 {
			return
		}
		//we randomly picked up failNum disks and mark as unavailable
		shuff := make([]int, len(e.diskInfos))
		for i := 0; i < len(e.diskInfos); i++ {
			shuff[i] = i
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
		log.Println("simulate on failure of:")
		for i := 0; i < failNum; i++ {
			fmt.Println(e.diskInfos[shuff[i]].diskPath)
			e.diskInfos[shuff[i]].available = false
		}
	} else if mode == "bitRot" {

	}
}

//update a file according to a new file, the local `filename` will be used to update the file in the cloud with the same name
func (e *Erasure) update(filename string) error {
	//two ways are available, one is RWM, other one is RCW
	//we minimize the parity computation and transferring overhead as much as possible
	oldFi, ok := e.fileMap[filename]
	if !ok {
		return ErrFileNotFound
	}
	//read new file
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	newFi := &FileInfo{fileName: filename}
	fileInfo, err := f.Stat()
	if err != nil {
		return err
	}
	//we allocate the buffer actually the same size of the file
	// fmt.Println(fileInfo)
	size := fileInfo.Size()
	newFi.fileSize = size
	buf := bufio.NewReader(f)
	data := make([]byte, size)
	_, err = buf.Read(data)
	if err != nil {
		return err
	}
	//if the len of local file is 0, we regard as deleting
	if len(data) == 0 {
		log.Println("The input file length is 0, this will lead to removal of present file")
		if ans, err := consultUserBeforeAction(); !ans {
			return nil
		} else if err != nil {
			return err
		}
		e.removeFile(filename)
		return e.writeConfig()
	}
	h := sha256.New()
	f.Seek(0, 0)
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	newFi.hash = fmt.Sprintf("%x", h.Sum(nil))

	//we split the data, and create empty parity
	newShards, err := e.enc.Split(data)
	if err != nil {
		return err
	}
	//we read old shards
	oldShards := make([][]byte, e.k)
	g := new(errgroup.Group)
	for i, path := range e.diskInfos {
		i := i
		path := path
		if oldFi.distribution[i] > e.k {
			continue
		}
		g.Go(func() error {
			filepath := path.diskPath + "/" + filename + "/D_" +
				fmt.Sprintf("%d", oldFi.distribution[i])
			f, err := os.Open(filepath)
			if err != nil {
				return err
			}
			defer f.Close()
			blockByte := (int(oldFi.fileSize) + e.k - 1) / e.k
			oldShards[oldFi.distribution[i]] = make([]byte, blockByte)
			f.Read(oldShards[oldFi.distribution[i]])
			if err != nil {
				return err
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	err = e.enc.Update(oldShards, newShards)
	if err != nil {
		return err
	}
	//if a block is updated, we replace it, otherwise remain intact
	//and all parity is bound to update

	//write into config
	err = e.writeConfig()
	if err != nil {
		return err
	}
	return nil
}
