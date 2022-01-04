package grasure

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

//test functionality of recover
//if encountered error "too many open files", use `ulimit -n` to enlarge the max ope
func TestRecover(t *testing.T) {
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
	tempFileSizes := generateRandomFileSize(1*KiB, 1*MiB, 10)
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
						testEC.Destroy(&SimOptions{Mode: "diskFail", FailNum: fn})
						//Don't worry, I'll fix with that
						rm, err := testEC.Recover(&Options{Degrade: false})
						if err != nil {
							if fn > m && err == errTooFewDisksAlive ||
								fn > totalDisk-testEC.DiskNum && err == errNotEnoughBackupForRecovery {
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

//PASS
func benchmarkRecover(b *testing.B, dataShards, parityShards, diskNum, failNum int, blockSize, fileMaxSize int64, fileNum int) {
	b.ResetTimer()
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
	tempFileSizes := generateRandomFileSize(1*KiB, fileMaxSize, fileNum)
	defer deleteTempFiles(tempFileSizes)
	//1. read disk paths
	err = testEC.ReadDiskPath()
	if err != nil {
		b.Fatal(err)
	}
	// for each tuple (k,m,N,bs) we testify  encoding
	// and decoding functions for numerous files

	//randomly generate some file and encode them into system
	testEC.K = dataShards
	testEC.M = parityShards
	testEC.DiskNum = diskNum
	testEC.BlockSize = blockSize
	err = testEC.InitSystem(true)
	if err != nil {
		b.Fatal(err)
	}
	totalFileSize := int64(0)
	for _, fileSize := range tempFileSizes {
		totalFileSize += fileSize
		inpath := fmt.Sprintf("input/temp-%d", fileSize)
		err = generateRandomFileBySize(inpath, fileSize)
		if err != nil {
			b.Fatal(err)
		}

		err = testEC.ReadConfig()
		if err != nil {
			b.Fatal(err)
		}
		_, err := testEC.EncodeFile(inpath)
		if err != nil {
			b.Fatal(err)
		}
		err = testEC.WriteConfig()
		if err != nil {
			b.Fatal(err)
		}

	}
	b.SetBytes(totalFileSize)
	// oops, serveral disks shut down one by one
	for i := 0; i < b.N; i++ {
		testEC.Destroy(&SimOptions{Mode: "diskFail", FailNum: failNum})
		//Don't worry, I'll fix with that
		_, err := testEC.Recover(&Options{Degrade: false})
		if err != nil {
			b.Fatal(err)
		}
		//restore diskConfigFile to previous content
		if err := os.Rename(testDiskFilePath+".old", testDiskFilePath); err != nil {
			b.Fatal(err)
		}
		err = testEC.ReadDiskPath()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// func benchmarkRecoverReadK(b *testing.B, dataShards, parityShards, diskNum, failNum int, blockSize, fileMaxSize int64, fileNum int) {
// 	b.ResetTimer()
// 	genTempDir()
// 	testEC := &Erasure{
// 		ConfigFile: "conf.json",
// 		// fileMap:         make(map[string]*fileInfo),
// 		DiskFilePath:    testDiskFilePath,
// 		ReplicateFactor: 3,
// 		ConStripes:      100,
// 		Override:        true,
// 		Quiet:           true,
// 	}
// 	rand.Seed(100000007)
// 	tempFileSizes := generateRandomFileSize(1*KiB, fileMaxSize, fileNum)
// 	defer deleteTempFiles(tempFileSizes)
// 	//1. read disk paths
// 	err = testEC.ReadDiskPath()
// 	if err != nil {
// 		b.Fatal(err)
// 	}
// 	// for each tuple (k,m,N,bs) we testify  encoding
// 	// and decoding functions for numerous files
// 	//randomly generate some file and encode them into system
// 	testEC.K = dataShards
// 	testEC.M = parityShards
// 	testEC.DiskNum = diskNum
// 	testEC.BlockSize = blockSize
// 	err = testEC.InitSystem(true)
// 	if err != nil {
// 		b.Fatal(err)
// 	}
// 	totalFileSize := int64(0)
// 	for _, fileSize := range tempFileSizes {
// 		totalFileSize += fileSize
// 		inpath := fmt.Sprintf("input/temp-%d", fileSize)
// 		err = generateRandomFileBySize(inpath, fileSize)
// 		if err != nil {
// 			b.Fatal(err)
// 		}

// 		err = testEC.ReadConfig()
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 		_, err := testEC.EncodeFile(inpath)
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 		err = testEC.WriteConfig()
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// 	b.SetBytes(totalFileSize)
// 	// oops, serveral disks shut down one by one
// 	for i := 0; i < b.N; i++ {
// 		testEC.Destroy("diskFail", failNum, "")
// 		//Don't worry, I'll fix with that
// 		_, err := testEC.RecoverReadK()
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 		//restore diskConfigFile to previous content
// 		if err := os.Rename(testDiskFilePath+".old", testDiskFilePath); err != nil {
// 			b.Fatal(err)
// 		}
// 		err = testEC.ReadDiskPath()
// 		if err != nil {
// 			b.Fatal(err)
// 		}
// 	}
// }
func BenchmarkReccover2x1x3x1x512x1Mx50(b *testing.B) {
	benchmarkRecover(b, 2, 1, 3, 1, 512, 1*MiB, 50)
}

func BenchmarkReccover2x1x4x1x512x1Mx20(b *testing.B) {
	benchmarkRecover(b, 2, 1, 4, 1, 512, 1*MiB, 20)
}

func BenchmarkReccover2x2x6x2x1024x1Mx50(b *testing.B) {
	benchmarkRecover(b, 2, 2, 6, 2, 1024, 1*MiB, 50)
}

func BenchmarkReccover4x2x6x2x4096x1Mx20(b *testing.B) {
	benchmarkRecover(b, 4, 2, 6, 2, 4096, 1*MiB, 20)
}

// func BenchmarkReccover4x2x6x2x4096x1Mx20ReadK(b *testing.B) {
// 	benchmarkRecoverReadK(b, 4, 2, 6, 2, 4096, 1*MiB, 20)
// }

func BenchmarkReccover4x3x8x3x4096x1Mx20(b *testing.B) {
	benchmarkRecover(b, 4, 3, 8, 3, 4096, 1*MiB, 20)
}

// func BenchmarkReccover4x3x8x3x4096x1Mx20ReadK(b *testing.B) {
// 	benchmarkRecoverReadK(b, 4, 3, 8, 3, 4096, 1*MiB, 20)
// }

func BenchmarkReccover5x2x8x2x8192x1Mx40(b *testing.B) {
	benchmarkRecover(b, 5, 2, 8, 2, 8192, 1*MiB, 40)
}

// func BenchmarkReccover5x2x8x2x8192x1Mx40ReadK(b *testing.B) {
// 	benchmarkRecoverReadK(b, 5, 2, 8, 2, 8192, 1*MiB, 40)
// }

func BenchmarkReccover6x3x9x3x8192x2Mx20(b *testing.B) {
	benchmarkRecover(b, 6, 3, 9, 3, 8192, 2*MiB, 20)
}

func BenchmarkReccover8x4x16x4x16384x1Mx10(b *testing.B) {
	benchmarkRecover(b, 8, 4, 16, 4, 16384, 1*MiB, 10)
}

func BenchmarkReccover9x3x16x2x8192x10Mx20(b *testing.B) {
	benchmarkRecover(b, 5, 2, 8, 2, 8192, 10*MiB, 20)
}

func BenchmarkReccover12x2x14x2x4096x10Mx20(b *testing.B) {
	benchmarkRecover(b, 12, 2, 14, 2, 4096, 10*MiB, 20)
}

// func BenchmarkReccover12x2x14x2x4096x10Mx20ReadK(b *testing.B) {
// 	benchmarkRecoverReadK(b, 12, 2, 14, 2, 4096, 10*MiB, 20)
// }

func BenchmarkReccover16x4x20x3x32768x10Mx20(b *testing.B) {
	benchmarkRecover(b, 16, 4, 20, 3, 32768, 10*MiB, 20)
}

// func BenchmarkReccover16x4x20x3x32768x10Mx20ReadK(b *testing.B) {
// 	benchmarkRecoverReadK(b, 16, 4, 20, 3, 32768, 10*MiB, 20)
// }

func BenchmarkReccover16x4x20x3x32768x1Mx200(b *testing.B) {
	benchmarkRecover(b, 16, 4, 20, 3, 32768, 1*MiB, 200)
}

// func BenchmarkReccover16x4x20x3x32768x1Mx200ReadK(b *testing.B) {
// 	benchmarkRecoverReadK(b, 16, 4, 20, 3, 32768, 1*MiB, 200)
// }
