package grasure

import (
	"fmt"
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
	//since we focus on recovering, we do not necessarily have to generate a dozen of files
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

					for failNum := 1; failNum <= testEC.M; failNum++ {
						// log.Printf("----k:%d,m:%d,bs:%d,N:%d,fail:%d----\n", k, m, bs, N, failNum)
						err = testEC.InitSystem(true)
						if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
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
						//Here we explicitly test single and multiple fault
						testEC.Destroy(&SimOptions{
							Mode:    "diskFail",
							FailNum: failNum,
						})
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false, WithSGA: true})
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

func TestWithoutSGA(t *testing.T) {
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
	//since we focus on recovering, we do not necessarily have to generate a dozen of files
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

					for failNum := 1; failNum <= testEC.M; failNum++ {
						// log.Printf("----k:%d,m:%d,bs:%d,N:%d,fail:%d----\n", k, m, bs, N, failNum)
						err = testEC.InitSystem(true)
						if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
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
						//Here we explicitly test single and multiple fault
						testEC.Destroy(&SimOptions{
							Mode:    "diskFail",
							FailNum: failNum,
						})
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

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade, WithSGA: true})
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

func benchmarkWithoutSGA(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
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

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade, WithSGA: false})
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

func BenchmarkWithSGA2x1x3x512x1M(b *testing.B) {
	benchmarkWithSGA(b, 2, 1, 3, 512, 1*MiB, 1, false)
}

func BenchmarkWithoutSGA2x1x3x512x1M(b *testing.B) {
	benchmarkWithoutSGA(b, 2, 1, 3, 512, 1*MiB, 1, false)
}

func BenchmarkWithSGA2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkWithSGA(b, 2, 2, 4, 1024, 1*MiB, 2, false)
}
func BenchmarkWithoutSGA2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkWithoutSGA(b, 2, 2, 4, 1024, 1*MiB, 2, false)
}

func BenchmarkWithSGA4x2x6x1024x1Mx1(b *testing.B) {
	benchmarkWithSGA(b, 4, 2, 6, 1024, 1*MiB, 1, false)
}
func BenchmarkWithoutSGA4x2x6x1024x1Mx1(b *testing.B) {
	benchmarkWithoutSGA(b, 4, 2, 6, 1024, 1*MiB, 1, false)
}

func BenchmarkWithSGA6x3x9x8192x10Mx3(b *testing.B) {
	benchmarkWithSGA(b, 6, 3, 9, 4096, 10*MiB, 3, false)
}
func BenchmarkWithoutSGA6x3x9x8192x10Mx3(b *testing.B) {
	benchmarkWithoutSGA(b, 6, 3, 9, 4096, 10*MiB, 3, false)
}

func BenchmarkWithSGA8x4x16x16384x10Mx4(b *testing.B) {
	benchmarkWithSGA(b, 8, 4, 16, 16384, 10*MiB, 4, false)
}
func BenchmarkWithoutSGA8x4x16x16384x10Mx4(b *testing.B) {
	benchmarkWithoutSGA(b, 8, 4, 16, 16384, 10*MiB, 4, false)
}

func BenchmarkWithSGA9x6x15x8192x10Mx3(b *testing.B) {
	benchmarkWithSGA(b, 9, 3, 16, 8192, 10*MiB, 3, false)
}
func BenchmarkWithoutSGA9x6x15x8192x10Mx3(b *testing.B) {
	benchmarkWithoutSGA(b, 9, 3, 15, 8192, 10*MiB, 3, false)
}

func BenchmarkWithSGA12x4x18x8192x10Mx2(b *testing.B) {
	benchmarkWithSGA(b, 12, 4, 18, 8192, 10*MiB, 2, false)
}
func BenchmarkWithoutSGA12x4x18x8192x10Mx2(b *testing.B) {
	benchmarkWithoutSGA(b, 12, 4, 18, 8192, 10*MiB, 2, false)
}

func BenchmarkWithSGA16x4x24x8192x10Mx3(b *testing.B) {
	benchmarkWithSGA(b, 16, 4, 24, 8192, 10*MiB, 3, false)
}
func BenchmarkWithoutSGA16x4x24x8192x10Mx3(b *testing.B) {
	benchmarkWithoutSGA(b, 16, 4, 24, 8192, 10*MiB, 3, false)
}

func BenchmarkWithSGA20x4x24x4096x20Mx4(b *testing.B) {
	benchmarkWithSGA(b, 20, 4, 24, 4096, 20*MiB, 4, false)
}
func BenchmarkWithoutSGA20x4x24x4096x20Mx4(b *testing.B) {
	benchmarkWithoutSGA(b, 20, 4, 24, 4096, 20*MiB, 4, false)
}

func benchmarkSGAParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum, conNum int, degrade bool) {
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
	//create c files and signal to channel
	for i := 0; i < conNum; i++ {
		inpath[i] = fmt.Sprintf("input/temp%d-%d", i, fileSize)
		outpath[i] = fmt.Sprintf("output/temp%d-%d", i, fileSize)
		err = generateRandomFileBySize(inpath[i], fileSize)
		if err != nil {
			b.Fatal(err)
		}
		//we encode the file in advance
		_, err := testEC.EncodeFile(inpath[i])
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
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

	testEC.Destroy(&SimOptions{Mode: "diskFail", FailNum: failNum})

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			i := <-fileCh

			err = testEC.ReadFile(inpath[i], outpath[i], &Options{Degrade: degrade})
			if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
			}
			fileCh <- i
		}
	})
}

func BenchmarkSGAParallel_2x2x4x1024x1Mx1x2(b *testing.B) {
	benchmarkSGAParallel(b, 2, 2, 4, 1024, 1*MiB, 1, 2, false)
}

func BenchmarkSGAParallel_4x2x6x1024x1Mx2x4(b *testing.B) {
	benchmarkSGAParallel(b, 4, 2, 6, 1024, 1*MiB, 2, 4, false)
}
