package grasure

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

//functionality tests
func TestWithSGA(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	genTempDir()
	testEC := &Erasure{
		ConfigFile:      "conf.json",
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
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
					err = testEC.InitSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					for failNum := 1; failNum <= testEC.M; failNum++ {
						// log.Printf("----k:%d,m:%d,bs:%d,N:%d,fail:%d----\n", k, m, bs, N, failNum)

						for _, disk := range testEC.diskInfos {
							disk.available = true
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

//PASS

func TestWithGCA(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	genTempDir()
	testEC := &Erasure{
		ConfigFile:      "conf.json",
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
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
					err = testEC.InitSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					for failNum := 1; failNum <= testEC.M; failNum++ {
						// log.Printf("----k:%d,m:%d,bs:%d,N:%d,fail:%d----\n", k, m, bs, N, failNum)

						for _, disk := range testEC.diskInfos {
							disk.available = true
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
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false, WithSGA: true, WithGCA: true})
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

func TestWithoutSGA(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
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
					err = testEC.InitSystem(true)
					if err != nil {
						t.Fatalf("k:%d,m:%d,bs:%d,N:%d,%s\n", k, m, bs, N, err.Error())
					}
					for failNum := 1; failNum <= testEC.M; failNum++ {
						// log.Printf("----k:%d,m:%d,bs:%d,N:%d,fail:%d----\n", k, m, bs, N, failNum)
						for _, disk := range testEC.diskInfos {
							disk.available = true
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

//benchmark tests
func benchmarkBaseline(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
	b.ResetTimer()
	b.SetBytes(fileSize)
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	//defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
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
	for i := 0; i < b.N; i++ {

		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade, WithSGA: false})
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

func benchmarkWithSGA(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
	b.ResetTimer()
	b.SetBytes(fileSize)
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	//defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
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

	for i := 0; i < b.N; i++ {

		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade, WithSGA: true})
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

//Note: GCA depends on SGA's outcome
func benchmarkWithGCA(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum int, degrade bool) {
	b.ResetTimer()
	b.SetBytes(fileSize)
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
		Override:        true,
		Quiet:           true,
	}
	rand.Seed(100000007)
	//defer deleteTempFiles([]int64{fileSize})
	inpath := filepath.Join("input", fmt.Sprintf("temp-%d", fileSize))
	outpath := filepath.Join("output", fmt.Sprintf("temp-%d", fileSize))
	err = generateRandomFileBySize(inpath, fileSize)
	if err != nil {
		b.Fatalf("k:%d,m:%d,bs:%d,N:%d,fs:%d, %s\n", dataShards, parityShards, blockSize, diskNum, fileSize, err.Error())
	}
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
	for i := 0; i < b.N; i++ {

		// log.Printf("----k:%d,m:%d,bs:%d,N:%d----\n", k, m, bs, N)

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: degrade, WithSGA: true, WithGCA: true})
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

func BenchmarkWithSGA2x1x3x512x1M(b *testing.B) {
	benchmarkWithSGA(b, 2, 1, 3, 512, 1*MiB, 1, false)
}

// 12          92156796 ns/op          11.38 MB/s     5630254 B/op      58242 allocs/op

func BenchmarkBaseline2x1x3x512x1M(b *testing.B) {
	benchmarkBaseline(b, 2, 1, 3, 512, 1*MiB, 1, false)
}

// 15          75593663 ns/op          13.87 MB/s     5097836 B/op      43303 allocs/op

func BenchmarkWithSGA2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkWithSGA(b, 2, 2, 4, 1024, 1*MiB, 2, false)
}

// 19          57017134 ns/op          18.39 MB/s     6695192 B/op   32267 allocs/op

func BenchmarkBaseline2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkBaseline(b, 2, 2, 4, 1024, 1*MiB, 2, false)
}

// 25          41051034 ns/op          25.54 MB/s     4664189 B/op      16626 allocs/op

func BenchmarkWithSGA4x2x6x1024x1Mx1(b *testing.B) {
	benchmarkWithSGA(b, 4, 2, 6, 1024, 1*MiB, 1, false)
}

// 21          50411945 ns/op          20.80 MB/s     6426600 B/op      24642 allocs/op

func BenchmarkBaseline4x2x6x1024x1Mx1(b *testing.B) {
	benchmarkBaseline(b, 4, 2, 6, 1024, 1*MiB, 1, false)
}

// 25          41051034 ns/op          25.54 MB/s     4664189 B/op      16626 allocs/op

func BenchmarkWithSGA6x3x9x8192x10Mx3(b *testing.B) {
	benchmarkWithSGA(b, 6, 3, 9, 4096, 10*MiB, 3, false)
}

// 6         186141556 ns/op          56.33 MB/s    38367601 B/op   50045 allocs/op

func BenchmarkBaseline6x3x9x8192x10Mx3(b *testing.B) {
	benchmarkBaseline(b, 6, 3, 9, 4096, 10*MiB, 3, false)
}

// 6         192607708 ns/op          54.44 MB/s    37685298 B/op    37770 allocs/op

func BenchmarkWithSGA8x4x16x16384x10Mx4(b *testing.B) {
	benchmarkWithSGA(b, 8, 4, 16, 16384, 10*MiB, 4, false)
}

// 7         150438667 ns/op          69.70 MB/s    46127457 B/op      14989 allocs/op

func BenchmarkBaseline8x4x16x16384x10Mx4(b *testing.B) {
	benchmarkBaseline(b, 8, 4, 16, 16384, 10*MiB, 4, false)
}

// 7         143706267 ns/op          72.97 MB/s    46014085 B/op    13577 allocs/op

func BenchmarkWithSGA9x6x15x8192x10Mx3(b *testing.B) {
	benchmarkWithSGA(b, 9, 3, 16, 8192, 10*MiB, 3, false)
}

// 6         171089483 ns/op          61.29 MB/s    37603145 B/op      23370 allocs/op

func BenchmarkBaseline9x6x15x8192x10Mx3(b *testing.B) {
	benchmarkBaseline(b, 9, 3, 15, 8192, 10*MiB, 3, false)
}

// 7         153507932 ns/op          68.31 MB/s    37061705 B/op    19396 allocs/op

func BenchmarkWithSGA12x4x18x8192x10Mx2(b *testing.B) {
	benchmarkWithSGA(b, 12, 4, 18, 8192, 10*MiB, 2, false)
}

// 7         183826931 ns/op          57.04 MB/s    46450907 B/op      23813 allocs/op

func BenchmarkBaseline12x4x18x8192x10Mx2(b *testing.B) {
	benchmarkBaseline(b, 12, 4, 18, 8192, 10*MiB, 2, false)
}

// 6         167069150 ns/op          62.76 MB/s    46391402 B/op    19670 allocs/op

func BenchmarkWithSGA16x4x24x8192x10Mx3(b *testing.B) {
	benchmarkWithSGA(b, 16, 4, 24, 8192, 10*MiB, 3, false)
}

// 7         150561632 ns/op          69.64 MB/s    38043987 B/op   22330 allocs/op

func BenchmarkBaseline16x4x24x8192x10Mx3(b *testing.B) {
	benchmarkBaseline(b, 16, 4, 24, 8192, 10*MiB, 3, false)
}

// 7         145379675 ns/op          72.13 MB/s    37838318 B/op    20437 allocs/op

func BenchmarkWithSGA20x4x24x4096x20Mx4(b *testing.B) {
	benchmarkWithSGA(b, 20, 4, 24, 4096, 20*MiB, 4, false)
}

// 4         294637462 ns/op          71.18 MB/s    56650320 B/op      74759 allocs/op

func BenchmarkBaseline20x4x24x4096x20Mx4(b *testing.B) {
	benchmarkBaseline(b, 20, 4, 24, 4096, 20*MiB, 4, false)
}

// 4         311492602 ns/op          67.33 MB/s    55941788 B/op    70832 allocs/op

// parallel tests
func benchmarkSGAParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum, conNum int, degrade bool) {
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
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

	//simulate disk failure
	testEC.Destroy(&SimOptions{Mode: "diskFail", FailNum: failNum})
	//set the channel
	rand.Seed(100000007)
	// create shards channel
	fileCh := make(chan int, conNum)
	inpath := make([]string, conNum)
	outpath := make([]string, conNum)
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

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			i := <-fileCh

			err = testEC.ReadFile(inpath[i], outpath[i], &Options{Degrade: degrade, WithSGA: true})
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

func benchmarkNoSGAParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, failNum, conNum int, degrade bool) {
	genTempDir()
	testEC := &Erasure{
		ConfigFile: "conf.json",
		// fileMap:         make(map[string]*fileInfo),
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      1,
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

	//simulate disk failure
	testEC.Destroy(&SimOptions{Mode: "diskFail", FailNum: failNum})
	//set the channel
	rand.Seed(100000007)
	// create shards channel
	fileCh := make(chan int, conNum)
	inpath := make([]string, conNum)
	outpath := make([]string, conNum)
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

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			i := <-fileCh

			err = testEC.ReadFile(inpath[i], outpath[i], &Options{Degrade: degrade, WithSGA: false})
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

func BenchmarkSGAParallel_2x2x4x1024x1Mx1x2(b *testing.B) {
	benchmarkSGAParallel(b, 2, 2, 4, 1024, 1*MiB, 1, 2, false)
}

// BenchmarkSGAParallel_2x2x4x1024x1Mx1x2-2              52          23245678 ns/op          90.22 MB/s     4499638 B/op    22087 allocs/op

func BenchmarkNoSGAParallel_2x2x4x1024x1Mx1x2(b *testing.B) {
	benchmarkNoSGAParallel(b, 2, 2, 4, 1024, 1*MiB, 1, 2, false)
}

//BenchmarkNoSGAParallel_2x2x4x1024x1Mx1x2-2            58          18589997 ns/op         112.81 MB/s     4195211 B/op    13800 allocs/op

func BenchmarkSGAParallel_4x2x6x1024x1Mx2x4(b *testing.B) {
	benchmarkSGAParallel(b, 4, 2, 6, 1024, 1*MiB, 2, 4, false)
}

// BenchmarkSGAParallel_4x2x6x1024x1Mx2x4-2              76          15370311 ns/op         272.88 MB/s     3062620 B/op    15088 allocs/op

func BenchmarkNoSGAParallel_4x2x6x1024x1Mx2x4(b *testing.B) {
	benchmarkNoSGAParallel(b, 4, 2, 6, 1024, 1*MiB, 2, 4, false)
}

// BenchmarkNoSGAParallel_4x2x6x1024x1Mx2x4-2            86          12129589 ns/op         345.79 MB/s     2830078 B/op     8895 allocs/op

//---Remind that GCA only works when disknum >> k+m, and m > 1
func BenchmarkWithGCA4x2x12x4096x10Mx1(b *testing.B) {
	benchmarkWithGCA(b, 4, 2, 12, 4096, 10*MiB, 2, false)
}

// 10         192867288 ns/op          54.37 MB/s    40110038 B/op      53985 allocs/op

func BenchmarkWithSGA4x2x12x4096x10Mx1(b *testing.B) {
	benchmarkWithSGA(b, 4, 2, 12, 4096, 10*MiB, 2, false)
}

// 10         102548710 ns/op         102.25 MB/s    23277325 B/op      36158 allocs/op

func BenchmarkBase4x2x12x4096x10Mx1(b *testing.B) {
	benchmarkBaseline(b, 4, 2, 12, 4096, 10*MiB, 2, false)
}

// 10         109229141 ns/op          96.00 MB/s    22783136 B/op      24066 allocs/op
