// This test unit tests the encoding and decoding efficiency
//
package grasure

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

//-------------------------TEST UNIT----------------------------

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
		inpath := fmt.Sprintf("./examples/test/temp-%d", fileSize)
		outpath := fmt.Sprintf("./examples/output/temp-%d", fileSize)
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

func deleteTempFileGroup(inpath, outpath []string) {
	for i := range inpath {
		if ex, _ := PathExist(inpath[i]); !ex {
			continue
		}
		err = os.Remove(inpath[i])
		if err != nil {
			log.Fatal(err)
		}
		if ex, _ := PathExist(outpath[i]); !ex {
			continue
		}
		err = os.Remove(outpath[i])
		if err != nil {
			log.Fatal(err)
		}
	}
}

func TestEncodeDecodeNormal(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.ReadDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDisk := len(testEC.diskInfos)
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards {
			testEC.M = m
			for N := k + m; N <= min(k+m+4, totalDisk); N++ {
				testEC.DiskNum = N
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.InitSystem(true)
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

						err = testEC.ReadConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.WriteConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.ReadFile(inpath, outpath)
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

//Test when one disk fails
func TestEncodeDecodeOneFailure(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.ReadDiskPath()
	if err != nil {
		t.Fatal(err)
	}
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
				testEC.DiskNum = N
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.InitSystem(true)
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

						err = testEC.ReadConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.WriteConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.ReadFile(inpath, outpath)
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
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.ReadDiskPath()
	if err != nil {
		t.Fatal(err)
	}
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
				testEC.DiskNum = N
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.InitSystem(true)
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

						err = testEC.ReadConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.WriteConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.ReadFile(inpath, outpath)
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

// Test remove func
func TestRemove(t *testing.T) {
	testEC := &Erasure{
		ConfigFile:      "conf.json",
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 100)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.ReadDiskPath()
	if err != nil {
		t.Fatal(err)
	}
	totalDisk := len(testEC.diskInfos)
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	for _, k := range dataShards {
		testEC.K = k
		for _, m := range parityShards {
			testEC.M = m
			for N := k + m; N <= min(k+m+4, totalDisk); N++ {
				testEC.DiskNum = N
				for _, bs := range blockSizesV1 {
					testEC.BlockSize = bs
					err = testEC.InitSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

					for _, fileSize := range tempFileSizes {
						inpath := fmt.Sprintf("./test/temp-%d", fileSize)
						err = generateRandomFileBySize(inpath, fileSize)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						err = testEC.ReadConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						_, err := testEC.EncodeFile(inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d encode fails when fileSize is %d, for %s", k, m, bs, N, fileSize, err.Error())
						}
						err = testEC.RemoveFile(inpath)
						if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.WriteConfig()
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}

						if ok, err := testEC.checkIfFileExist(inpath); err != errFileBlobNotFound || ok {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())

						}
					}
				}
			}
		}
	}

}

//PASS
//---------------------BENCHMARK---------------------------------
//Benchmarks dataShards, parityShards, diskNum, blockSize, fileSize
func benchmarkEncodeDecode(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64) {
	b.ResetTimer()
	b.SetBytes(fileSize)

	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	defer deleteTempFiles([]int64{fileSize})
	inpath := fmt.Sprintf("./test/temp-%d", fileSize)
	outpath := fmt.Sprintf("./output/temp-%d", fileSize)
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
	//repeat b.N times
	for i := 0; i < b.N; i++ {
		err = testEC.ReadDiskPath()
		if err != nil {
			b.Fatal(err)
		}
		// for each tuple (k,m,N,bs) we testify  encoding
		// and decoding functions for numerous files
		testEC.K = dataShards
		testEC.M = parityShards
		testEC.DiskNum = diskNum
		testEC.BlockSize = blockSize
		err = testEC.InitSystem(true)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.ReadConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		_, err := testEC.EncodeFile(inpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		err = testEC.WriteConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		err = testEC.ReadFile(inpath, outpath)
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
	b.SetBytes(fileSize)
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
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
		err = testEC.ReadDiskPath()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < failNum; j++ {
			testEC.diskInfos[j].available = false
		}
		// for each tuple (k,m,N,bs) we testify  encoding
		// and decoding functions for numerous files
		testEC.K = dataShards
		testEC.M = parityShards
		testEC.DiskNum = diskNum

		testEC.BlockSize = blockSize
		err = testEC.InitSystem(true)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.ReadConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		_, err := testEC.EncodeFile(inpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
		err = testEC.WriteConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		err = testEC.ReadFile(inpath, outpath)
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

// test performance when multiple users send encode/read requests.
func benchmarkParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, conNum int) {
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
		DiskFilePath:    "examples/.hdr.disks.path",
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	err = testEC.ReadDiskPath()
	if err != nil {
		b.Fatal(err)
	}
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files
	testEC.K = dataShards
	testEC.M = parityShards
	testEC.DiskNum = diskNum

	testEC.BlockSize = blockSize
	err = testEC.InitSystem(true)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}

	//set the channel
	rand.Seed(100000007)
	// create shards channel
	fileCh := make(chan int, conNum)
	inpath := make([]string, conNum)
	outpath := make([]string, conNum)
	//create c files and sent to channel
	for i := 0; i < conNum; i++ {
		inpath[i] = fmt.Sprintf("./test/temp%d-%d", i, fileSize)
		outpath[i] = fmt.Sprintf("./output/temp%d-%d", i, fileSize)
		err = generateRandomFileBySize(inpath[i], fileSize)
		if err != nil {
			b.Fatal(err)
		}
		fileCh <- i
	}
	defer deleteTempFileGroup(inpath, outpath)
	b.SetBytes(fileSize * int64(conNum))
	b.SetParallelism(conNum)
	b.ReportAllocs()
	b.ResetTimer()
	//start the benchmark goroutines
	err = testEC.ReadConfig()
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
	defer func() {
		err = testEC.WriteConfig()
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
	}()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			i := <-fileCh

			_, err := testEC.EncodeFile(inpath[i])
			if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			}

			err = testEC.ReadFile(inpath[i], outpath[i])
			if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			}
			//evaluate the results
			if ok, err := checkFileIfSame(inpath[i], outpath[i]); !ok && err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			} else if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			}
			fileCh <- i
		}
	})
}

func BenchmarkParallel_2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkParallel(b, 2, 2, 4, 1024, 1*MiB, 2)
}
func BenchmarkParallel_4x2x6x1024x1Mx4(b *testing.B) {
	benchmarkParallel(b, 4, 2, 6, 1024, 1*MiB, 4)
}

func BenchmarkParallel_2x3x6x4096x1Mx4(b *testing.B) {
	benchmarkParallel(b, 2, 3, 6, 4096, 1*MiB, 4)
}

func BenchmarkParallel_4x3x8x4096x1Mx8(b *testing.B) {
	benchmarkParallel(b, 4, 3, 8, 4096, 1*MiB, 8)
}

func BenchmarkParallel_6x3x9x4096x5Mx3(b *testing.B) {
	benchmarkParallel(b, 6, 3, 9, 4096, 5*MiB, 3)
}

func BenchmarkParallel_12x4x18x8192x10Mx12(b *testing.B) {
	benchmarkParallel(b, 12, 4, 18, 8192, 10*MiB, 12)
}

func BenchmarkParallel_16x4x24x8192x10Mx40(b *testing.B) {
	benchmarkParallel(b, 16, 4, 24, 8192, 10*MiB, 40)
}

func BenchmarkParallel_20x4x24x16384x10Mx80(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 80)
}

func BenchmarkParallel_20x4x24x16384x10Mx100(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 100)
}

func BenchmarkParallel_20x4x24x16384x1Mx200(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 200)
}
