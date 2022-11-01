package grasure

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
)

var updateMode = []int{
	1, 2, 3,
}

//-------------------------TEST UNIT----------------------------

// mode: 1 exchange; 2 append; 3 delete
func changeRandom(filePath string, fileSize, num, mode int) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	data := make([]byte, fileSize)
	buf.Read(data)

	num = min(num, fileSize)

	switch mode {
	case 1:
		for i := 0; i < num; i++ {
			index := rand.Int() % len(data)
			newbyte := rand.Int()
			data[index] = byte(newbyte)
		}
	case 2:
		for i := 0; i < num; i++ {
			newbyte := rand.Int()
			data = append(data, byte(newbyte))
		}
	case 3:
		index := rand.Int() % len(data)
		if len(data)-index > num {
			data = append(data[:index], data[index+num:]...)
		} else {
			data = data[num:]
		}
	}

	fw, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer fw.Close()

	w := bufio.NewWriter(fw)
	w.WriteString(string(data))
	if err != nil {
		panic(err)
	}
	w.Flush()

	return nil
}

func TestUpdateNormalExchange(t *testing.T) {
	// we generate temp data and encode it into real storage sytem
	// then change the file content randomly, and update it
	// after that, all temporary file should be deleted
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
	// for each tuple (k, m, N, bs) we testify update
	// functions for numerous files
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
						inpath := fmt.Sprintf("./input/temp-%d", fileSize)
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

						changeRandom(inpath, int(fileSize), int(fileSize/20), 1)
						err = testEC.Update(inpath, inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,mode:%d update fails when fileSize is %d, for %s", k, m, bs, N, 1, fileSize, err.Error())
						}
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", k, m, bs, N, 1, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err == nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, 1, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", k, m, bs, N, 1, fileSize, err.Error())
						}
					}
				}
			}
		}
	}
}

func TestUpdateNormalAppend(t *testing.T) {
	// we generate temp data and encode it into real storage sytem
	// then change the file content randomly, and update it
	// after that, all temporary file should be deleted
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
	// for each tuple (k, m, N, bs) we testify update
	// functions for numerous files
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
						inpath := fmt.Sprintf("./input/temp-%d", fileSize)
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

						changeRandom(inpath, int(fileSize), int(fileSize/20), 2)
						err = testEC.Update(inpath, inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,mode:%d update fails when fileSize is %d, for %s", k, m, bs, N, 2, fileSize, err.Error())
						}
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", k, m, bs, N, 2, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err == nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, 2, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", k, m, bs, N, 2, fileSize, err.Error())
						}
					}
				}
			}
		}
	}
}

func TestUpdateNormalDelete(t *testing.T) {
	// we generate temp data and encode it into real storage sytem
	// then change the file content randomly, and update it
	// after that, all temporary file should be deleted
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
	// for each tuple (k, m, N, bs) we testify update
	// functions for numerous files
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
						inpath := fmt.Sprintf("./input/temp-%d", fileSize)
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

						changeRandom(inpath, int(fileSize), int(fileSize/20), 3)
						err = testEC.Update(inpath, inpath)
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,mode:%d update fails when fileSize is %d, for %s", k, m, bs, N, 3, fileSize, err.Error())
						}
						err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
						if err != nil {
							t.Errorf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", k, m, bs, N, 3, fileSize, err.Error())
						}

						//evaluate the results
						if ok, err := checkFileIfSame(inpath, outpath); !ok && err == nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for hash check fail", k, m, bs, N, 3, fileSize)
						} else if err != nil {
							t.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", k, m, bs, N, 3, fileSize, err.Error())
						}
					}
				}
			}
		}
	}
}

// ---------------------BENCHMARK---------------------------------
func benchmarkUpdate(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64) {
	genTempDir()
	b.ResetTimer()
	b.SetBytes(fileSize)
	testEC := &Erasure{
		ConfigFile:      "conf.json",
		DiskFilePath:    testDiskFilePath,
		ReplicateFactor: 3,
		ConStripes:      100,
		Override:        true,
		Quiet:           true,
	}
	defer deleteTempFiles([]int64{fileSize})
	inpath := fmt.Sprintf("./input/temp-%d", fileSize)
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

		mode := 1
		// for _, mode := range updateMode {
		changeRandom(inpath, int(fileSize), int(fileSize)/10, mode)

		err = testEC.Update(inpath, inpath)
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d update fails when fileSize is %d, for %s", dataShards, parityShards, blockSize, diskNum, mode, fileSize, err.Error())
		}

		err = testEC.ReadFile(inpath, outpath, &Options{Degrade: false})
		if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", dataShards, parityShards, blockSize, diskNum, mode, fileSize, err.Error())
		}

		//evaluate the results
		if ok, err := checkFileIfSame(inpath, outpath); !ok && err == nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for hash check fail", dataShards, parityShards, blockSize, diskNum, mode, fileSize)
		} else if err != nil {
			b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", dataShards, parityShards, blockSize, diskNum, mode, fileSize, err.Error())
		}
		// }

	}
}

func BenchmarkUpdate2x1x3x512x1M(b *testing.B) {
	benchmarkUpdate(b, 2, 1, 3, 512, 1*MiB)
}

func BenchmarkUpdate2x2x4x1024x1M(b *testing.B) {
	benchmarkUpdate(b, 2, 2, 4, 1024, 1*MiB)
}

func BenchmarkUpdate2x3x6x4096x1M(b *testing.B) {
	benchmarkUpdate(b, 2, 3, 6, 4096, 1*MiB)
}

func BenchmarkUpdate4x2x6x1024x1M(b *testing.B) {
	benchmarkUpdate(b, 4, 2, 6, 1024, 1*MiB)
}

func BenchmarkUpdate4x3x8x4096x1M(b *testing.B) {
	benchmarkUpdate(b, 4, 3, 8, 4096, 1*MiB)
}

func BenchmarkUpdate5x3x10x4096x5M(b *testing.B) {
	benchmarkUpdate(b, 5, 3, 10, 4096, 5*MiB)
}

func BenchmarkUpdate6x3x9x8192x10M(b *testing.B) {
	benchmarkUpdate(b, 6, 3, 9, 4096, 10*MiB)
}

func BenchmarkUpdate8x4x16x16384x10M(b *testing.B) {
	benchmarkUpdate(b, 8, 4, 16, 16384, 10*MiB)
}

func BenchmarkUpdate9x3x16x8192x10M(b *testing.B) {
	benchmarkUpdate(b, 9, 3, 16, 8192, 10*MiB)
}

func BenchmarkUpdate12x4x18x8192x10M(b *testing.B) {
	benchmarkUpdate(b, 12, 4, 18, 8192, 10*MiB)
}

func BenchmarkUpdate14x4x20x4096x10M(b *testing.B) {
	benchmarkUpdate(b, 14, 4, 20, 4096, 10*MiB)
}

func BenchmarkUpdate16x4x24x8192x10M(b *testing.B) {
	benchmarkUpdate(b, 16, 4, 24, 8192, 10*MiB)
}

func BenchmarkUpdate20x4x24x4096x20M(b *testing.B) {
	benchmarkUpdate(b, 20, 4, 24, 4096, 20*MiB)
}

func benchmarkUpdateParallel(b *testing.B, dataShards, parityShards, diskNum int, blockSize, fileSize int64, conNum int) {
	genTempDir()
	testEC := &Erasure{
		ConfigFile:      "conf.json",
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
		inpath[i] = fmt.Sprintf("./input/temp%d-%d", i, fileSize)
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

			mode := 1
			changeRandom(inpath[i], int(fileSize), int(fileSize)/2, mode)

			err = testEC.Update(inpath[i], inpath[i])
			if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d update fails when fileSize is %d, for %s", dataShards, parityShards, blockSize, diskNum, mode, fileSize, err.Error())
			}

			err = testEC.ReadFile(inpath[i], outpath[i], &Options{Degrade: false})
			if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", dataShards, parityShards, blockSize, diskNum, mode, fileSize, err.Error())
			}

			//evaluate the results
			if ok, err := checkFileIfSame(inpath[i], outpath[i]); !ok && err == nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for hash check fail", dataShards, parityShards, blockSize, diskNum, mode, fileSize)
			} else if err != nil {
				b.Fatalf("k:%d,m:%d,bs:%d,N:%d,mode:%d read fails when fileSize is %d, for %s", dataShards, parityShards, blockSize, diskNum, mode, fileSize, err.Error())
			}
			fileCh <- i
		}
	})
}

func BenchmarkUpdateParallel_2x2x4x1024x1Mx2(b *testing.B) {
	benchmarkUpdateParallel(b, 2, 2, 4, 1024, 1*MiB, 2)
}
func BenchmarkUpdateParallel_4x2x6x1024x1Mx4(b *testing.B) {
	benchmarkUpdateParallel(b, 4, 2, 6, 1024, 1*MiB, 4)
}

func BenchmarkUpdateParallel_2x3x6x4096x1Mx4(b *testing.B) {
	benchmarkUpdateParallel(b, 2, 3, 6, 4096, 1*MiB, 4)
}

func BenchmarkUpdateParallel_4x3x8x4096x1Mx8(b *testing.B) {
	benchmarkUpdateParallel(b, 4, 3, 8, 4096, 1*MiB, 8)
}

func BenchmarkUpdateParallel_6x3x9x4096x5Mx3(b *testing.B) {
	benchmarkUpdateParallel(b, 6, 3, 9, 4096, 5*MiB, 3)
}

func BenchmarkUpdateParallel_12x4x18x8192x10Mx12(b *testing.B) {
	benchmarkUpdateParallel(b, 12, 4, 18, 8192, 10*MiB, 12)
}

func BenchmarkUpdateParallel_16x4x24x8192x10Mx40(b *testing.B) {
	benchmarkUpdateParallel(b, 16, 4, 24, 8192, 10*MiB, 40)
}

func BenchmarkUpdateParallel_20x4x24x16384x10Mx80(b *testing.B) {
	benchmarkUpdateParallel(b, 20, 4, 24, 16384, 10*MiB, 80)
}

func BenchmarkUpdateParallel_20x4x24x16384x10Mx100(b *testing.B) {
	benchmarkUpdateParallel(b, 20, 4, 24, 16384, 10*MiB, 100)
}

func BenchmarkUpdateParallel_20x4x24x16384x1Mx200(b *testing.B) {
	benchmarkUpdateParallel(b, 20, 4, 24, 16384, 10*MiB, 200)
}
