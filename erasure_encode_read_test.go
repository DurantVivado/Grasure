// This test unit tests the encoding and decoding efficiency
//
package grasure

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/DurantVivado/reedsolomon"
)

//-------------------------TEST UNIT----------------------------

//genTempDir creates /input and /output dir in workspace root
func genTempDir() {
	if ok, err := pathExist("input"); !ok && err == nil {
		if err := os.Mkdir("input", 0644); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}
	if ok, err := pathExist("output"); !ok && err == nil {
		if err := os.Mkdir("output", 0644); err != nil {
			log.Fatal(err)
		}
	} else if err != nil {
		log.Fatal(err)
	}
}

//generateRandomFileSize generate `num` files within range [minSize, maxSize]
func generateRandomFileSize(minSize, maxSize int64, num int) []int64 {
	out := make([]int64, num)
	for i := 0; i < num; i++ {
		out[i] = rand.Int63()%(maxSize-minSize) + minSize
	}
	return out
}

// generateRandomFileBySize generates a named  file with `fileSize` bytes.
func generateRandomFileBySize(filename string, fileSize int64) error {

	if ex, err := pathExist(filename); ex && err == nil {
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

//deleteTempFiles deletes temporary generated files as well as folders
func deleteTempFiles(tempFileSizes []int64) {
	for _, fileSize := range tempFileSizes {
		inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
		outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
		if ex, _ := pathExist(inpath); !ex {
			continue
		}
		err = os.Remove(inpath)
		if err != nil {
			log.Fatal(err)
		}
		if ex, _ := pathExist(outpath); !ex {
			continue
		}
		err = os.Remove(outpath)
		if err != nil {
			log.Fatal(err)
		}
	}
}

//deleteTempFilesGroup deletes temporary generated file groups
func deleteTempFileGroup(inpath, outpath []string) {
	for i := range inpath {
		if ex, _ := pathExist(inpath[i]); !ex {
			continue
		}
		err = os.Remove(inpath[i])
		if err != nil {
			log.Fatal(err)
		}
		if ex, _ := pathExist(outpath[i]); !ex {
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
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
						outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))

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

						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
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
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
						outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
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
	genTempDir()
	testEC := &Erasure{
		ConfigFile:      "conf.json",
		DiskFilePath:    testDiskFilePath,
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
						outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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

						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
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

func TestEncodeDecodeBitRot(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
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
			for N := k + m + 1; N <= min(k+m+4, totalDisk); N++ {
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
						outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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
						randFail := int(rand.Int31()) % (k + m)
						testEC.Destroy(&SimOptions{Mode: "bitRot", FailNum: randFail, FileName: inpath})

						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
						if err != nil {
							if randFail > m && err == reedsolomon.ErrTooFewShards {
								continue
							}
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

//Test degraded read when one disk fails
func TestEncodeDecodeOneFailureDegraded(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
						outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: true})
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

//Test degraded read when two disk fails
func TestEncodeDecodeTwoFailureDegraded(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
						outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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

						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: true})
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
	genTempDir()
	testEC := &Erasure{
		ConfigFile:      "conf.json",
		DiskFilePath:    testDiskFilePath,
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
						inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
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
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		//evaluate the results
		// if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
		// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		// } else if err != nil {
		// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		// }
	}
}
func benchmarkEncodeDecodeWithFault(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
	b.ResetTimer()
	b.SetBytes(fileSize)
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade})
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		//evaluate the results
		// if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
		// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		// } else if err != nil {
		// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		// }
	}
}

func BenchmarkEncodeDecode2x1x3x512x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 2, 1, 3, 512, 1*MiB)
}
func BenchmarkEncodeDecode2x2x4x1024x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 2, 2, 4, 1024, 1*MiB)
}

func BenchmarkEncodeDecode2x2x4x1024x1Mx1fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 2, 2, 4, 1024, 1*MiB, 1, false)
}
func BenchmarkEncodeDecode2x2x4x1024x1Mx2fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 2, 2, 4, 1024, 1*MiB, 2, false)
}
func BenchmarkEncodeDecode2x2x4x1024x1Mx1faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 2, 2, 4, 1024, 1*MiB, 1, true)
}
func BenchmarkEncodeDecode2x2x4x1024x1Mx2faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 2, 2, 4, 1024, 1*MiB, 2, true)
}

func BenchmarkEncodeDecode2x3x6x4096x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 2, 3, 6, 4096, 1*MiB)
}

func BenchmarkEncodeDecode4x2x6x1024x1M(b *testing.B) {
	benchmarkEncodeDecode(b, 4, 2, 6, 1024, 1*MiB)
}
func BenchmarkEncodeDecode4x2x6x1024x1Mx1fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 4, 2, 6, 1024, 1*MiB, 1, false)
}
func BenchmarkEncodeDecode4x2x6x1024x1Mx2fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 4, 2, 6, 1024, 1*MiB, 2, false)
}
func BenchmarkEncodeDecode4x2x6x1024x1Mx1faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 4, 2, 6, 1024, 1*MiB, 1, true)
}
func BenchmarkEncodeDecode4x2x6x1024x1Mx2faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 4, 2, 6, 1024, 1*MiB, 2, true)
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
	benchmarkEncodeDecodeWithFault(b, 6, 3, 9, 4096, 10*MiB, 2, false)
}

func BenchmarkEncodeDecode6x3x9x8192x10Mx3fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 6, 3, 9, 4096, 10*MiB, 3, false)
}
func BenchmarkEncodeDecode6x3x9x8192x10Mx2faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 6, 3, 9, 4096, 10*MiB, 2, true)
}

func BenchmarkEncodeDecode6x3x9x8192x10Mx3faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 6, 3, 9, 4096, 10*MiB, 3, true)
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
	benchmarkEncodeDecodeWithFault(b, 20, 4, 24, 4096, 20*MiB, 2, false)
}

func BenchmarkEncodeDecode20x4x24x4096x20Mx4fault(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 20, 4, 24, 4096, 20*MiB, 4, false)
}
func BenchmarkEncodeDecode20x4x24x4096x20Mx2faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 20, 4, 24, 4096, 20*MiB, 2, true)
}

func BenchmarkEncodeDecode20x4x24x4096x20Mx4faultxdegrade(b *testing.B) {
	benchmarkEncodeDecodeWithFault(b, 20, 4, 24, 4096, 20*MiB, 4, true)
}

// test performance when multiple users send encode/read requests.
func benchmarkParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, conNum int, degrade bool) {
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
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
		inpath[i] = fmt.Sprintf("input/temp%d-%d", i, fileSize)
		outpath[i] = fmt.Sprintf("output/temp%d-%d", i, fileSize)
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

			err = testEC.ReadFile(inpath[i], outpath[i], &Options{Degrade: degrade})
			if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			}
			//evaluate the results
			// if ok, err := checkFileIfSame(inpath[i], outpath[i]); !ok && err != nil {
			// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			// } else if err != nil {
			// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			// }
			fileCh <- i
		}
	})
}

func BenchmarkParallel_2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkParallel(b, 2, 2, 4, 1024, 1*MiB, 2, false)
}
func BenchmarkParallel_4x2x6x1024x1Mx4(b *testing.B) {
	benchmarkParallel(b, 4, 2, 6, 1024, 1*MiB, 4, false)
}
func BenchmarkParallel_4x2x6x1024x1Mx4xdegrade(b *testing.B) {
	benchmarkParallel(b, 4, 2, 6, 1024, 1*MiB, 4, true)
}

func BenchmarkParallel_2x3x6x4096x1Mx4(b *testing.B) {
	benchmarkParallel(b, 2, 3, 6, 4096, 1*MiB, 4, false)
}

func BenchmarkParallel_2x3x6x4096x1Mx4xdegrade(b *testing.B) {
	benchmarkParallel(b, 2, 3, 6, 4096, 1*MiB, 4, true)
}

func BenchmarkParallel_4x3x8x4096x1Mx8(b *testing.B) {
	benchmarkParallel(b, 4, 3, 8, 4096, 1*MiB, 8, false)
}
func BenchmarkParallel_4x3x8x4096x1Mx8xdegrade(b *testing.B) {
	benchmarkParallel(b, 4, 3, 8, 4096, 1*MiB, 8, true)
}

func BenchmarkParallel_6x3x9x4096x5Mx3(b *testing.B) {
	benchmarkParallel(b, 6, 3, 9, 4096, 5*MiB, 3, false)
}
func BenchmarkParallel_6x3x9x4096x5Mx3xdegrade(b *testing.B) {
	benchmarkParallel(b, 6, 3, 9, 4096, 5*MiB, 3, true)
}

func BenchmarkParallel_12x4x18x8192x10Mx12(b *testing.B) {
	benchmarkParallel(b, 12, 4, 18, 8192, 10*MiB, 12, false)
}
func BenchmarkParallel_12x4x18x8192x10Mx12xdegrade(b *testing.B) {
	benchmarkParallel(b, 12, 4, 18, 8192, 10*MiB, 12, true)
}

func BenchmarkParallel_16x4x24x8192x10Mx40(b *testing.B) {
	benchmarkParallel(b, 16, 4, 24, 8192, 10*MiB, 40, false)
}
func BenchmarkParallel_16x4x24x8192x10Mx40xdegrade(b *testing.B) {
	benchmarkParallel(b, 16, 4, 24, 8192, 10*MiB, 40, true)
}
func BenchmarkParallel_20x4x24x16384x10Mx80(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 80, false)
}

func BenchmarkParallel_20x4x24x16384x10Mx80xdegrade(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 80, true)
}

func BenchmarkParallel_20x4x24x16384x10Mx100(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 100, false)
}
func BenchmarkParallel_20x4x24x16384x10Mx100xdegrade(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 16384, 10*MiB, 100, true)
}

func BenchmarkParallel_20x4x24x4096x1Mx200(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 4096, 10*MiB, 200, false)
}
func BenchmarkParallel_20x4x24x4096x1Mx200xdegrade(b *testing.B) {
	benchmarkParallel(b, 20, 4, 24, 4096, 10*MiB, 200, true)
}

//the impact of conStripe
func benchmarkEncodeDecodeConstripe(b *testing.B, conStripe, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
	b.ResetTimer()
	b.SetBytes(fileSize)
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      conStripe,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
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

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade})
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}

		//evaluate the results
		// if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
		// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		// } else if err != nil {
		// 	b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		// }
	}
}

func BenchmarkEncodeDecode20x4x24x4096x20Mx2x50(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 50, 20, 4, 24, 4096, 20*MiB, 2, false)
}

//6         192236068 ns/op         109.09 MB/s    49596322 B/op      61852 allocs/op

func BenchmarkEncodeDecode20x4x24x4096x20Mx2x100(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 100, 20, 4, 24, 4096, 20*MiB, 2, false)
}

// 5         208078144 ns/op         100.79 MB/s    54397798 B/op      61763 allocs/op

func BenchmarkEncodeDecode20x4x24x4096x20Mx2x150(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 150, 20, 4, 24, 4096, 20*MiB, 2, false)
}

// 6         190064891 ns/op         110.34 MB/s    57785693 B/op      61863 allocs/op

func BenchmarkEncodeDecode20x4x24x4096x20Mx2x200(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 200, 20, 4, 24, 4096, 20*MiB, 2, false)
}

// 6         201086254 ns/op         104.29 MB/s    71690058 B/op      61534 allocs/op

func BenchmarkEncodeDecode20x4x24x4096x20Mx2x400(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 400, 20, 4, 24, 4096, 20*MiB, 2, false)
}

// 6	 188624099 ns/op	 111.18 MB/s	88068084 B/op	   61723 allocs/op

func BenchmarkEncodeDecode12x4x16x4096x10Mx2x1(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 1, 12, 4, 16, 4096, 10*MiB, 2, false)
}

// 8         146975893 ns/op          71.34 MB/s    24668448 B/op      33026 allocs/op

func BenchmarkEncodeDecode12x4x16x4096x10Mx2x10(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 10, 12, 4, 16, 4096, 10*MiB, 2, false)
}

// 8         135243915 ns/op          77.53 MB/s    25510278 B/op      32915 allocs/op

func BenchmarkEncodeDecode12x4x16x4096x10Mx2x50(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 50, 12, 4, 16, 4096, 10*MiB, 2, false)
}

//       10	 108872997 ns/op	  96.31 MB/s	29188593 B/op	   33150 allocs/op

func BenchmarkEncodeDecode12x4x16x4096x10Mx2x100(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 100, 12, 4, 16, 4096, 10*MiB, 2, false)
}

// 10	 107138339 ns/op	  97.87 MB/s	34925734 B/op	   33191 allocs/op

func BenchmarkEncodeDecode12x4x16x4096x10Mx2x150(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 150, 12, 4, 16, 4096, 10*MiB, 2, false)
}

// 10         108390984 ns/op          96.74 MB/s    37385000 B/op      33259 allocs/op

func BenchmarkEncodeDecode12x4x16x4096x10Mx2x200(b *testing.B) {
	benchmarkEncodeDecodeConstripe(b, 200, 12, 4, 16, 4096, 10*MiB, 2, false)
}

// 9         113679860 ns/op          92.24 MB/s    46519827 B/op      33543 allocs/op

//how to sweep the variable paremeter to obtain optimal value
