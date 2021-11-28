// This test unit tests the encoding and decoding efficiency
//
package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

//randomly generate file of different size and encode them into HDR system
const (
	KiB = 1 << 10
	MiB = 1 << 20
	GiB = 1 << 30
	TiB = 1 << 40
)

var dataShards = []int{
	2, 4, 6, 8, 10, 12, 14, 16, 18, 20,
}
var parityShards = []int{
	1, 2, 3, 4,
}

type encodeTest []struct {
	dataBlocks   int
	parityBlocks int
	diskNum      int
	blockSize    int64
	// missingData, missingParity int
	// reconstructParity          bool
	// shouldFail                 bool
}

var fileSizesV1 = []int64{
	128, 256, 512, 1024,
	128 * KiB, 256 * KiB, 512 * KiB,
	1 * MiB, 4 * MiB, 16 * MiB, 32 * MiB, 64 * MiB,
}
var fileSizesV2 = []int64{

	128 * MiB, 256 * MiB, 512 * MiB, 1024 * MiB,
}
var blockSizesV1 = []int64{
	1 * KiB, 2 * KiB, 4 * KiB, 8 * KiB, 16 * KiB, 32 * KiB, 64 * KiB, 128 * KiB,
	256 * KiB, 512 * KiB,
}

var blockSizesV2 = []int64{
	1 * MiB, 2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB, 32 * MiB, 64 * MiB, 128 * MiB,
	256 * MiB,
}
var bigFilePaths = []string{
	"./test/file.1G",
	"./test/file.4G",
	"./test/file.8G",
	"./test/file.16G",
}

// func TestInit(t *testing.T) {

// 	err = testEC.initHDR()
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// }
func generateRandomFileSize(minSize, maxSize int64, num int) []int64 {
	out := make([]int64, num)
	for i := 0; i < num; i++ {
		out[i] = rand.Int63()%(maxSize-minSize) + minSize
	}
	return out
}
func generateRandomFileBySize(filename string, fileSize int64) error {

	if ex, err := PathExist(filename); ex && err == nil {
		return nil
	} else if err != nil {
		return err
	}
	buf := make([]byte, fileSize)
	fillRandom(buf)
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(buf)
	if err != nil {
		return err
	}
	return nil
}
func TestEncodeDecode(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	//fileSize:
	//Group1: 128, 256, 512 ,1024
	//Group2: 4k, 8k, 16k, 32k, ...,1024K
	//Group3: 1M, 2M, 4M, 8M, ...,1024M
	//Group4: 1G, 4G, 8G, 16G
	//we open file and write data
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer func() {
		for _, fileSize := range tempFileSizes {
			inpath := fmt.Sprintf("./test/temp-%d", fileSize)
			if ex, err := PathExist(inpath); !ex && err == nil {
				return
			} else if err != nil {
				t.Fatal(err)
			}
			err = os.Remove(inpath)
			if err != nil {
				t.Fatal(err)
			}
		}
	}()
	err = testEC.readDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDiskInfo := testEC.diskInfos
	totalDisk := len(testEC.diskInfos)
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards {
			testEC.M = m
			for N := k + m; N <= totalDisk; N++ {
				testEC.diskInfos = totalDiskInfo[:N]
				for _, blockSize := range blockSizesV1 {
					testEC.BlockSize = blockSize
					err = testEC.resetSystem()
					if err != nil {
						t.Fatal(err)
					}
					for _, fileSize := range tempFileSizes {
						//system-level paras
						inpath := fmt.Sprintf("./test/temp-%d", fileSize)
						outpath := fmt.Sprintf("./output/temp-%d", fileSize)
						err = generateRandomFileBySize(inpath, fileSize)
						if err != nil {
							t.Fatal(err)
						}

						// t.Logf("k:%d,m:%d fails when fileSize is %d, for %s", k, m, fileSize, err.Error())
						err = testEC.readConfig()
						if err != nil {
							t.Fatal(err)
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d encode fails when fileSize is %d, for %s", k, m, blockSize, fileSize, err.Error())
						}
						err = erasure.writeConfig()
						if err != nil {
							t.Fatal(err)
						}
						err = erasure.updateConfigReplica()
						if err != nil {
							t.Fatal(err)
						}

						//simulate failure of disks
						for fail := 0; fail < m; fail++ {
							erasure.destroy("diskFail", fail)
							err = erasure.readFile(inpath, outpath)
							if err != nil {
								t.Fatalf("k:%d,m:%d,bs:%d read fails when fileSize is %d, for %s", k, m, blockSize, fileSize, err.Error())
							}

						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
							t.Errorf("k:%d,m:%d,bs:%d read fails when fileSize is %d, for hash check fail", k, m, blockSize, fileSize)
						} else if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d read fails when fileSize is %d, for %s", k, m, blockSize, fileSize, err.Error())
						}
					}
				}
			}
		}
	}

}
