package grasure

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestRecover(t *testing.T) {
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*FileInfo),
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

	//randomly generate some file and encode them into system
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
						inpath := fmt.Sprintf("input/temp-%d", fileSize)
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

					}
					// oops, serveral disks shut down one by one
					for fn := 1; fn <= m+1; fn++ {
						testEC.Destroy("diskFail", fn)
						//Don't worry, I'll fix with that
						rm, err := testEC.Recover()
						if err != nil {
							if fn == m+1 && err == errTooFewDisksAlive {
								err = testEC.ReadDiskPath()
								if err != nil {
									t.Fatal(err)
								}
								continue
							}
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,fn:%d,%s\n", k, m, bs, N, fn, err.Error())
						}
						// check if the resumed blobs are consistent with former ones
						for old, new := range rm {
							for _, fileSize := range tempFileSizes {
								oldPath := filepath.Join(old, fmt.Sprintf("temp-%d", fileSize), "BLOB")
								newPath := filepath.Join(new, fmt.Sprintf("temp-%d", fileSize), "BLOB")
								if ok, err := checkFileIfSame(newPath, oldPath); !ok && err != nil {
									t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
								} else if err != nil {
									t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
								}
							}
						}
						//restore diskConfigFile to previous content
						if err := os.Rename(testDiskFilePath+".old", testDiskFilePath); err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
						}
						err = testEC.ReadDiskPath()
						if err != nil {
							t.Fatal(err)
						}
					}
				}

			}
		}
	}

}
