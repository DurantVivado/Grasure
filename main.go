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
	k          int                 // the number of data blocks in a stripe
	m          int                 // the number of parity blocks in a stripe
	enc        reedsolomon.Encoder // the reedsolomon encoder
	diskInfos  []*DiskInfo         //disk paths
	configFile string              //configure file
	fileLists  []*FileInfo         //File info lists
	rwmu       sync.RWMutex        //read write mutex
}
type FileInfo struct {
	fileName     string       //file name
	fileSize     int64        //file size
	metaInfo     *os.FileInfo //system-level file info
	hash         string       //hash value (SHA256 by default)
	distribution []int        //distribution represents the block replacement respect to disks
}

//the parameter lists
var mode = flag.String("mode", "encode", "the mode of ec system, one of (encode, decode, update, scaling, recover)")
var k = flag.Int("k", 12, "the number of data shards(<256)")
var m = flag.Int("m", 4, "the number of parity shards(2-4)")
var diskPath = flag.String("diskPath", "", "the disks path")
var file = flag.String("file", "", "the file path")
var savePath = flag.String("savePath", "file.save", "the local saving path for file")
var newFile = flag.String("newfile", "", "the new file path")
var oldFile = flag.String("oldfile", "", "the old file path")
var new_k = flag.Int("new_k", 16, "the new number of data shards(<256)")
var new_m = flag.Int("new_m", 4, "the new number of parity shards(2-4)")
var recoveredDiskPath = flag.String("recoveredDiskPath", "/tmp/data", "the data path for recovered disk, default to /tmp/data")
var failMode = flag.String("failMode", "diskFail", "simulate diskFail or bitRot mode")
var failNum = flag.Int("failNum", 2, "simulate multiple disk failure, provides the fail number of disks")
var override = flag.Bool("override", false, "whether to override former files or directories")

//Error definitions
var ErrConfFileNotExist = errors.New("the conf file not exist")
var ErrEmptyData = errors.New("the file to encode is empty")
var ErrDataDirExist = errors.New("data directory already exists")
var ErrTooFewDisks = errors.New("too few disks, i.e., k+m < N")
var ErrNotInitialized = errors.New("system not initialized, please initialize with `-mode init` first")
var ErrFileNotFound = errors.New("file not found")
var ErrSurvialNotEnoughForDecoding = errors.New("the failed parity number exceeds fault tolerance, data renders unrecoverable")
var ErrFileIncompleted = errors.New("file hash check fails, file renders incompleted")
var ErrFailModeNotRecognized = errors.New("the fail mode is not recognizable, please specify in \"diskFail\" or \"bitRot\"")

//read the config info in config file
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
	//next is the file lists
	//filename
	//hash
	//distribution
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

//We write the erasure parameters into config files
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
	//Read the file lists

	buf.Flush()
	f.Sync()
	return nil
}

//reset the storage assets
func (e *Erasure) reset() error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for _, path := range e.diskInfos {
		path := path
		files, err := os.ReadDir(path.diskPath)
		if err != nil {
			return err
		}
		if len(files) == 0 {
			continue
		}
		go func() {
			for _, file := range files {
				err = os.RemoveAll(path.diskPath + "/" + file.Name())
				wg.Done()

			}
		}()
	}
	wg.Wait()
	return nil
}

//update the file lists in conf file
func (e *Erasure) updateFileLists(fi *FileInfo) error {
	//we add file info to config file
	cf, err := os.OpenFile(e.configFile, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer cf.Close()
	//we find if the item list contains filename, if false, append it
	//to the end of the file, otherwise update the metas
	bfReader := bufio.NewReader(cf)
	pos := int64(0)
	for {
		line, err := bfReader.ReadString('\n')

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if strings.Contains(line, fi.fileName) {
			//update the next few lines
			pos += int64(len(line))
			rep := fmt.Sprintf("%d\t%s\n%v\n", fi.fileSize, fi.hash, fi.distribution)
			cf.WriteAt([]byte(rep), pos)
			log.Printf("%s successfully updated(replaced)", fi.fileName)
			return nil
		}
		pos += int64(len(line))

	}
	//append
	rep := fmt.Sprintf("%s\t%d\n%s\n%v\n", fi.fileName, fi.fileSize, fi.hash, fi.distribution)
	cf.WriteAt([]byte(rep), pos)
	log.Printf("%s successfully updated(appended)", fi.fileName)
	cf.Sync()
	return nil
}

//read meta data if a file exists, otherwise return nil,false
func (e *Erasure) readFileMeta(filename string) (*FileInfo, error) {
	cf, err := os.Open(e.configFile)
	if err != nil {
		return nil, err
	}
	defer cf.Close()
	bfReader := bufio.NewReader(cf)
	pos := int64(0)
	for {
		line, err := bfReader.ReadString('\n')

		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if strings.Contains(line, filename) {
			fi := &FileInfo{}
			fi.fileName = filename
			ss := strings.Split(strings.TrimSuffix(line, "\n"), "\t")
			fi.fileSize, _ = strconv.ParseInt(ss[1], 10, 64)
			//read next line
			line, err := bfReader.ReadString('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			fi.hash = strings.TrimSuffix(line, "\n")
			line, err = bfReader.ReadString('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			line = strings.Trim(line, "[]\n")
			for _, s := range strings.Split(line, " ") {
				num, err := strconv.Atoi(s)
				if err != nil {
					return nil, err
				}
				fi.distribution = append(fi.distribution, num)
			}
			return fi, nil
		}
		pos += int64(len(line))

	}
	return nil, ErrFileNotFound
}

var err error

func main() {
	//We read each file and make byte flow
	flag.Parse()
	//We read the config file
	erasure := new(Erasure)
	erasure.configFile = ".hdr.sys"
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
		//wait for user input
		// inputReader := bufio.NewReader(os.Stdin)
		for {
			// ans, err := inputReader.ReadString('\n')
			// if err != nil {
			// 	log.Fatal(err)
			// }
			ans := "Y"
			ans = strings.TrimSuffix(ans, "\n")
			if ans == "Y" || ans == "y" || ans == "Yes" || ans == "yes" {
				break
			} else if ans == "N" || ans == "n" || ans == "No" || ans == "no" {
				return
			} else {
				fmt.Println("Please do not make joke")
			}
		}
		failOnErr(*mode, err)
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
		err = erasure.read(*file, *savePath)
		failOnErr(*mode, err)
	case "encode":
		//We are entering the encoding mode, and for brevity,we only encode one file
		err = erasure.readConfig()
		failOnErr(*mode, err)
		err = erasure.readDiskPath(diskPathFile)
		failOnErr(*mode, err)
		fi, err := erasure.encode(*file)
		failOnErr(*mode, err)
		err = erasure.updateFileLists(fi)
		failOnErr(*mode, err)
	case "decode":
		//simulate disk failure and bitrot
		err = erasure.readDiskPath(diskPathFile)
		failOnErr(*mode, err)
		erasure.destroy(*failMode, *failNum)
		err := erasure.decode(*file)
		failOnErr(*mode, err)

	// case "update":
	// 	//update an old file according to a new file
	// 	e.readConfig()
	// 	update(*newFile, *oldFile)
	// case "scaling":
	// 	//scaling the system, ALERT: this is a system-level operation and irreversible
	// 	e.readConfig()
	// 	scaling(new_k, new_m)
	// case "recover":
	// 	//recover all the blocks of a disk and put the recovered result to new path
	// 	e.readConfig()
	// 	recover(*recoveredDiskPath)
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
	fi.metaInfo = &fileInfo
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
	e.fileLists = append(e.fileLists, fi)
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
		nf, err := os.OpenFile(partPath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
		defer nf.Close()
		buf := bufio.NewWriter(nf)
		_, err = buf.Write(shards[shuff[i]])
		if err != nil {
			return nil, err
		}

	}

	return fi, nil
}

//read file on the system and return byte stream
func (e *Erasure) read(filename string, savepath string) error {
	//1. we find if it is recorded on the conf
	fi, err := e.readFileMeta(filename)
	if err != nil {
		return err
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
	dataBytes := make([][]byte, e.k)
	if len(survivalData) == e.k {
		//no need for reconstructing
		for _, ind := range survivalData {
			ind := ind
			g.Go(func() error {
				filepath := e.diskInfos[ind].diskPath + "/" + filename + "/D_" + fmt.Sprintf("%d", fi.distribution[ind])
				f, err := os.Open(filepath)
				if err != nil {
					return err
				}
				defer f.Close()
				blockByte := (int(fi.fileSize) + e.k - 1) / e.k
				dataBytes[fi.distribution[ind]] = make([]byte, blockByte)
				f.Read(dataBytes[fi.distribution[ind]])
				if err != nil {
					return err
				}
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		f, err := os.OpenFile(savepath, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		defer f.Close()
		err = e.enc.Join(f, dataBytes, int(fi.fileSize))
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
		log.Printf("%s successfully read (Joined)!", filename)
		return nil
	}
	if len(survivalData)+len(survivalParity) < e.k {
		return ErrSurvialNotEnoughForDecoding
	}
	//We need to decode the file using parity
	parityBytes := make([][]byte, e.m)
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
			if fi.distribution[ind] < e.k {
				dataBytes[fi.distribution[ind]] = make([]byte, blockByte)
				_, err := f.Read(dataBytes[fi.distribution[ind]])
				if err != nil {
					return err
				}
			} else {
				parityBytes[fi.distribution[ind]] = make([]byte, blockByte)
				_, err := f.Read(parityBytes[fi.distribution[ind]])
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
	//reconstructing, we first decode data, once completed
	//notify the customer, and parity reconstruction, we move it to back-end
	err = e.enc.ReconstructData(dataBytes)
	if err != nil {
		return err
	}
	g.Go(func() error {
		err = e.enc.ReconstructParity(parityBytes)
		if err != nil {
			return err
		}
		return nil
	})
	f, err := os.OpenFile(savepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	err = e.enc.Join(f, dataBytes[:e.k], int(fi.fileSize))
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
	//wait for completion of back-end parity reconstruction
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

//simulate disk failure or bitrot
func (e *Erasure) destroy(mode string, failNum int) {
	if mode == "diskFail" {
		//we randomly picked up failNum disks and mark as unavailable
		shuff := make([]int, len(e.diskInfos))
		for i := 0; i < len(e.diskInfos); i++ {
			shuff[i] = i
		}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(shuff), func(i, j int) { shuff[i], shuff[j] = shuff[j], shuff[i] })
		for i := 0; i < failNum; i++ {
			e.diskInfos[shuff[i]].available = false
		}
	} else if mode == "bitRot" {

	}
}

//decode standalone file, according to survival parts
func (e *Erasure) decode(filename string, fi *FileInfo) error {
	//if the parity is lost ,the file can still be read and written
	//but if the data is lost, the file has to be read under degraded mode
	//where we must decode the data
	//randomly pick up k blocks for file reconstruction

	return nil
}
