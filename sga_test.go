package grasure

import (
	"fmt"
	"log"
	"math/rand"
	"path/filepath"
	"testing"
)

//we fail certain number of disks
func TestWithSGA(t *testing.T) {
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
	fileSize := int64(26269586)
	defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		t.Fatal(err)
	}
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

					for failNum := 1; failNum <= testEC.M; failNum++ {
						log.Printf("----k:%d,m:%d,bs:%d,N:%d,fail:%d----\n", k, m, bs, N, failNum)
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
						//Here we explicitly test single and multiple fault
						testEC.Destroy("diskFail", "", failNum, "")
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false, withSGA: true})
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

func benchmarkWithSGA(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
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
	testEC.Destroy(&SimOptions{Mode: "diskFail", FailNum: failNum})
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
		if ok, err := checkFileIfSame(inpath, outpath); !ok && err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		} else if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
		}
	}

}
