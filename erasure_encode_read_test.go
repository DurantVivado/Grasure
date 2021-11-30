// This test unit tests the encoding and decoding efficiency
//
package main

import (
	"fmt"
	"log"
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
	2, 3, 4, 5, 8, 9, 12, 14, 16, 18, 20,
}
var parityShards = []int{
	1, 2, 3, 4,
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
	4 * KiB, 16 * KiB, 64 * KiB,
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

func deleteTempFiles(tempFileSizes []int64) {
	for _, fileSize := range tempFileSizes {
		inpath := fmt.Sprintf("./test/temp-%d", fileSize)
		outpath := fmt.Sprintf("./output/temp-%d", fileSize)
		if ex, _ := PathExist(inpath); !ex {
			continue
		}
		err = os.Remove(inpath)
		if err != nil {
			log.Fatal(err)
		}
		if ex, _ := PathExist(outpath); !ex {
			continue
		}
		err = os.Remove(outpath)
		if err != nil {
			log.Fatal(err)
		}
	}
}
func TestEncodeDecodeNormal(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
		override:        true,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.readDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDiskInfo := testEC.diskInfos
	totalDisk := len(testEC.diskInfos)
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards {
			testEC.M = m
			for N := k + m; N <= min(k+m+4, totalDisk); N++ {
				testEC.diskInfos = totalDiskInfo[:N]
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.initSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

					for _, fileSize := range tempFileSizes {
						inpath := fmt.Sprintf("./test/temp-%d", fileSize)
						outpath := fmt.Sprintf("./output/temp-%d", fileSize)
						err = generateRandomFileBySize(inpath, fileSize)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.writeConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.updateConfigReplica()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readFile(inpath, outpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
					}
				}
			}
		}
	}

}

// PASS
//Test parallel
func TestEncodeDecodeNormalParallel(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
		override:        true,
	}

	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.readDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDiskInfo := testEC.diskInfos
	totalDisk := len(testEC.diskInfos)
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards {
			testEC.M = m
			for N := k + m; N <= min(k+m+4, totalDisk); N++ {
				testEC.diskInfos = totalDiskInfo[:N]
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.initSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

					for _, fileSize := range tempFileSizes {
						inpath := fmt.Sprintf("./test/temp-%d", fileSize)
						outpath := fmt.Sprintf("./output/temp-%d", fileSize)
						err = generateRandomFileBySize(inpath, fileSize)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.writeConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.updateConfigReplica()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readFile(inpath, outpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
					}
				}
			}
		}
	}

}

//Test when one disk fails
func TestEncodeDecodeOneFailure(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
	}
	override = true
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.readDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDiskInfo := testEC.diskInfos
	totalDisk := len(testEC.diskInfos)
	//simulate one disk failure
	testEC.diskInfos[0].available = false
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards {
			testEC.M = m
			for N := k + m; N <= min(k+m+4, totalDisk); N++ {
				testEC.diskInfos = totalDiskInfo[:N]
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.initSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

					for _, fileSize := range tempFileSizes {
						//system-level paras
						inpath := fmt.Sprintf("./test/temp-%d", fileSize)
						outpath := fmt.Sprintf("./output/temp-%d", fileSize)
						err = generateRandomFileBySize(inpath, fileSize)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.writeConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.updateConfigReplica()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readFile(inpath, outpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
					}
				}
			}
		}
	}

}

//PASS

//Test Parallel requests from clients

//Test when two disk fails
func TestEncodeDecodeTwoFailure(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
		override:        true,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.readDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDiskInfo := testEC.diskInfos
	totalDisk := len(testEC.diskInfos)
	//simulate two disk failure
	testEC.diskInfos[0].available = false
	testEC.diskInfos[1].available = false
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards[1:] {
			testEC.M = m
			for N := k + m; N <= min(k+m+4, totalDisk); N++ {
				testEC.diskInfos = totalDiskInfo[:N]
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.initSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

					for _, fileSize := range tempFileSizes {
						//system-level paras
						inpath := fmt.Sprintf("./test/temp-%d", fileSize)
						outpath := fmt.Sprintf("./output/temp-%d", fileSize)
						err = generateRandomFileBySize(inpath, fileSize)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.writeConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.updateConfigReplica()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.readFile(inpath, outpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d read fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
					}
				}
			}
		}
	}

}

//PASS

//Benchmarks dataShards, parityShards, diskNum, blockSize, fileSize
func benchmarkEncodeDecode(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64) {
	b.ResetTimer()
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
		override:        true,
	}
	rand.Seed(100000007)
	defer deleteTempFiles([]int64{fileSize})
	inpath := fmt.Sprintf("./test/temp-%d", fileSize)
	outpath := fmt.Sprintf("./output/temp-%d", fileSize)
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
	//repeat b.N times
	for i := 0; i < b.N; i++ {
		err = testEC.readDiskPath()
		if err != nil {
			b.Fatal(err)
		}
		totalDiskInfo := testEC.diskInfos
		// for each tuple (k,m,N,bs) we testify  encoding
		// and decoding functions for numerous files
		testEC.K = dataShards
		testEC.M = parityShards
		testEC.DiskNum = diskNum
		testEC.diskInfos = totalDiskInfo[:diskNum]
		testEC.BlockSize = blockSize
		err = testEC.initSystem(true)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.readConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		_, err := testEC.EncodeFile(inpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		err = testEC.writeConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		err = testEC.updateConfigReplica()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		err = testEC.readFile(inpath, outpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		//evaluate the results
		if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		} else if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
	}
}
func benchmarkEncodeDecodeWithFault(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int) {
	b.ResetTimer()
	testEC := &Erasure{
		configFile:      "conf.json",
		fileMap:         make(map[string]*FileInfo),
		diskFilePath:    ".hdr.disks.path",
		replicateFactor: 3,
		conStripes:      100,
		override:        true,
	}
	rand.Seed(100000007)
	defer deleteTempFiles([]int64{fileSize})
	inpath := fmt.Sprintf("./test/temp-%d", fileSize)
	outpath := fmt.Sprintf("./output/temp-%d", fileSize)
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
	//repeat b.N times

	for i := 0; i < b.N; i++ {
		err = testEC.readDiskPath()
		if err != nil {
			b.Fatal(err)
		}
		totalDiskInfo := testEC.diskInfos
		for j := 0; j < failNum; j++ {
			totalDiskInfo[j].available = false
		}
		// for each tuple (k,m,N,bs) we testify  encoding
		// and decoding functions for numerous files
		testEC.K = dataShards
		testEC.M = parityShards
		testEC.DiskNum = diskNum
		testEC.diskInfos = totalDiskInfo[:diskNum]

		testEC.BlockSize = blockSize
		err = testEC.initSystem(true)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.readConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		_, err := testEC.EncodeFile(inpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		err = testEC.writeConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		err = testEC.updateConfigReplica()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		err = testEC.readFile(inpath, outpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		//evaluate the results
		if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		} else if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
	}
}

func BenchmarkEncodeDecode2x1x3x512x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 2, 1, 3, 512, 1*MiB)
}
func BenchmarkEncodeDecode2x2x4x1024x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 2, 2, 4, 1024, 1*MiB)
}

func BenchmarkEncodeDecode2x2x4x1024x1Mx1fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 2, 2, 4, 1024, 1*MiB, 1)
}
func BenchmarkEncodeDecode2x2x4x1024x1Mx2fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 2, 2, 4, 1024, 1*MiB, 2)
}
func BenchmarkEncodeDecode2x3x6x4096x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 2, 3, 6, 4096, 1*MiB)
}

func BenchmarkEncodeDecode4x2x6x1024x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 4, 2, 6, 1024, 1*MiB)
}
func BenchmarkEncodeDecode4x2x6x1024x1Mx1fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 4, 2, 6, 1024, 1*MiB, 1)
}
func BenchmarkEncodeDecode4x2x6x1024x1Mx2fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 4, 2, 6, 1024, 1*MiB, 2)
}

func BenchmarkEncodeDecode4x3x8x4096x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 4, 3, 8, 4096, 1*MiB)
}

func BenchmarkEncodeDecode5x3x10x4096x5M(b *testing.B) {
	benchmarkEncodeDecode(b, 5, 3, 10, 4096, 5*MiB)
}

func BenchmarkEncodeDecode6x3x9x8192x10M(b *testing.B) {
	benchmarkEncodeDecode(b, 6, 3, 9, 4096, 10*MiB)
}
func BenchmarkEncodeDecode6x3x9x8192x10Mx2fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 6, 3, 9, 4096, 10*MiB, 2)
}

func BenchmarkEncodeDecode6x3x9x8192x10Mx3fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 6, 3, 9, 4096, 10*MiB, 3)
}
func BenchmarkEncodeDecode8x4x16x16384x10M(b *testing.B) {
	benchmarkEncodeDecode(b, 8, 4, 16, 16384, 10*MiB)
}

func BenchmarkEncodeDecode9x3x16x8192x10M(b *testing.B) {
	benchmarkEncodeDecode(b, 9, 3, 16, 8192, 10*MiB)
}

func BenchmarkEncodeDecode12x4x18x8192x10M(b *testing.B) {
	benchmarkEncodeDecode(b, 12, 4, 18, 8192, 10*MiB)
}

func BenchmarkEncodeDecode14x4x20x4096x10M(b *testing.B) {
	benchmarkEncodeDecode(b, 14, 4, 20, 4096, 10*MiB)
}

func BenchmarkEncodeDecode16x4x24x8192x10M(b *testing.B) {
	benchmarkEncodeDecode(b, 16, 4, 24, 8192, 10*MiB)
}

func BenchmarkEncodeDecode20x4x24x4096x20M(b *testing.B) {
	benchmarkEncodeDecode(b, 20, 4, 24, 4096, 20*MiB)
}

func BenchmarkEncodeDecode20x4x24x4096x20Mx2fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 20, 4, 24, 4096, 20*MiB, 2)
}

func BenchmarkEncodeDecode20x4x24x4096x20Mx4fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 20, 4, 24, 4096, 20*MiB, 4)
}

func benchmarkParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64) {

}
