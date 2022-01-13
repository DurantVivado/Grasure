package grasure_test

import (
	"crypto/sha256"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"

	grasure "github.com/DurantVivado/Grasure"
)

func fillRandom(p []byte) {
	for i := 0; i < len(p); i += 7 {
		val := rand.Int63()
		for j := 0; i+j < len(p) && j < 7; j++ {
			p[i+j] = byte(val)
			val >>= 8
		}
	}
}

func prepareDir(diskNum int) error {
	//1. diskConfigFile
	f, err := os.Create(".hdr.disks.path")
	if err != nil {
		return err
	}
	defer f.Close()
	//create some directories as file storage
	for i := 0; i < diskNum; i++ {
		path := fmt.Sprintf("disk%d", i)
		if err := os.RemoveAll(path); err != nil {
			return err
		}
		if err := os.Mkdir(path, 0644); err != nil {
			return err
		}
		_, err := f.WriteString(path + "\n")
		if err != nil {
			return err
		}
	}
	return nil

}
func delDir(diskNum int) error {
	//1. diskConfigFile
	//create some directories as file storage
	for i := 0; i < diskNum; i++ {
		path := fmt.Sprintf("disk%d", i)
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	if err := os.Remove(".hdr.disks.path"); err != nil {
		return err
	}
	if err := os.Remove("example.file"); err != nil {
		return err
	}
	if err := os.Remove("example.file.decode"); err != nil {
		return err
	}
	return nil

}

//An intriguing example of how to encode a file into the system
func ExampleErasure_EncodeFile() {
	// Create some sample data
	data := make([]byte, 250000)
	filepath := "example.file"
	fillRandom(data)
	// write it into a file
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
	}
	_, err = f.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	f.Close()
	// define the struct Erasure
	erasure := &grasure.Erasure{
		DiskFilePath:    ".hdr.disks.path",
		ConfigFile:      "config.json",
		DiskNum:         10,
		K:               6,
		M:               3,
		BlockSize:       4096,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
	}
	err = prepareDir(13)
	if err != nil {
		log.Fatal(err)
	}
	//read the disk paths
	err = erasure.ReadDiskPath()
	if err != nil {
		log.Fatal(err)
	}
	//first init the system
	err = erasure.InitSystem(true)
	if err != nil {
		log.Fatal(err)
	}
	//read the config file (auto-generated)
	err = erasure.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}
	//encode the file into system
	_, err = erasure.EncodeFile(filepath)
	if err != nil {
		log.Fatal(err)
	}
	//write the config
	err = erasure.WriteConfig()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("encode ok!")
	//Output:
	// Warning: you are intializing a new erasure-coded system, which means the previous data will also be reset.
	// System init!
	//  Erasure parameters: dataShards:6, parityShards:3,blocksize:4096,diskNum:10
	// encode ok!
}

//A canonical example of how to read a file normally from the system
func ExampleErasure_ReadFile_a() {
	filepath := "example.file"
	savePath := "example.file.decode"
	erasure := &grasure.Erasure{
		DiskFilePath:    ".hdr.disks.path",
		ConfigFile:      "config.json",
		DiskNum:         10,
		K:               6,
		M:               3,
		BlockSize:       4096,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
	}
	//read the disk paths
	err := erasure.ReadDiskPath()
	if err != nil {
		log.Fatal(err)
	}
	// read the config file
	err = erasure.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}
	// read the file and save to savePath
	err = erasure.ReadFile(filepath, savePath, &grasure.Options{Degrade: false})
	if err != nil {
		log.Fatal(err)
	}
	//check if two file are same
	if ok, err := checkFileIfSame(filepath, savePath); !ok && err != nil {
		log.Fatal(err)
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println("read ok!")
	//Output:
	// read ok!
}

//A heuristical example on read file in case of double failure
func ExampleErasure_ReadFile_b() {
	filepath := "example.file"
	savePath := "example.file.decode"
	erasure := &grasure.Erasure{
		DiskFilePath:    ".hdr.disks.path",
		ConfigFile:      "config.json",
		DiskNum:         10,
		K:               6,
		M:               3,
		BlockSize:       4096,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
	}
	//read the disk paths
	err := erasure.ReadDiskPath()
	if err != nil {
		log.Fatal(err)
	}
	// read the config file
	err = erasure.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}
	erasure.Destroy(&grasure.SimOptions{Mode: "diskFail", FailNum: 2})
	err = erasure.ReadFile(filepath, savePath, &grasure.Options{})
	if err != nil {
		log.Fatal(err)
	}
	if ok, err := checkFileIfSame(filepath, savePath); !ok && err != nil {
		log.Fatal(err)
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println("read ok!")
	//Output:
	// read ok!
}

//A curious example on removal of file, please encode the file into system first
func ExampleErasure_RemoveFile() {
	filepath := "example.file"
	erasure := &grasure.Erasure{
		DiskFilePath:    ".hdr.disks.path",
		ConfigFile:      "config.json",
		DiskNum:         10,
		K:               6,
		M:               3,
		BlockSize:       4096,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
	}
	//read the disk paths
	err := erasure.ReadDiskPath()
	if err != nil {
		log.Fatal(err)
	}
	err = erasure.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}
	err = erasure.RemoveFile(filepath)
	if err != nil {
		log.Fatal(err)
	}
	err = erasure.WriteConfig()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("file removed")
	//Output:
	//file removed
}

//A fabulous example on recovery of disks
func ExampleErasure_Recover() {
	erasure := &grasure.Erasure{
		DiskFilePath:    ".hdr.disks.path",
		ConfigFile:      "config.json",
		DiskNum:         10,
		K:               6,
		M:               3,
		BlockSize:       4096,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
	}
	//read the disk paths
	err := erasure.ReadDiskPath()
	if err != nil {
		log.Fatal(err)
	}
	err = erasure.ReadConfig()
	if err != nil {
		log.Fatal(err)
	}
	erasure.Destroy(&grasure.SimOptions{Mode: "diskFail", FailNum: 2})
	_, err = erasure.Recover(&grasure.Options{})
	if err != nil {
		log.Fatal(err)
	}
	err = erasure.WriteConfig()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("system recovered")
	//Output:
	//system recovered
}

func checkFileIfSame(dst, src string) (bool, error) {
	if ok, err := pathExist(dst); err != nil || !ok {
		return false, err
	}
	if ok, err := pathExist(src); err != nil || !ok {
		return false, err
	}
	fdst, err := os.Open(dst)
	if err != nil {
		return false, err
	}
	defer fdst.Close()
	fsrc, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer fsrc.Close()
	hashDst, err := hashStr(fdst)
	if err != nil {
		return false, err
	}
	hashSrc, err := hashStr(fsrc)
	if err != nil {
		return false, err
	}
	return hashDst == hashSrc, nil
}

//get a file's hash (shasum256)
func hashStr(f *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	out := fmt.Sprintf("%x", h.Sum(nil))
	return out, nil
}

//look if path exists
func pathExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
