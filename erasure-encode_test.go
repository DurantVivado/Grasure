// This test unit tests the encoding and decoding efficiency
//
package main

import (
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

var fileSizes = []int64{
	128, 256, 512, 1024,
	128 * KiB, 256 * KiB, 512 * KiB, 1024 * KiB,
	128 * MiB, 256 * MiB, 512 * MiB, 1024 * MiB,
}

var bigFilePaths = []string{
	"./test/file.1G",
	"./test/file.4G",
	"./test/file.8G",
	"./test/file.16G",
}

func TestEncode4096(t *testing.T) {
	//we generate temp data and encode it into real storage sytem
	//after that, all temporary file should be deleted
	//fileSize:
	//Group1: 128, 256, 512 ,1024
	//Group2: 4k, 8k, 16k, 32k, ...,1024K
	//Group3: 1M, 2M, 4M, 8M, ...,1024M
	//Group4: 1G, 4G, 8G, 16G
	//we open file and write data
	testEC := &Erasure{
		BlockSize: 4096,
	}
	for _, fileSize := range fileSizes {
		//system-level paras
		fileSize := fileSize
		buf := make([]byte, fileSize)
		fillRandom(buf)
		f, err := os.OpenFile(tempFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		for _, k := range dataShards {
			for _, m := range parityShards {
				testEC.K = k
				testEC.M = m
				t.Logf("k:%d,m:%d fails when fileSize is %d, for %s", k, m, fileSize, err.Error())

				_, err := testEC.EncodeFile(tempFile)
				if err != nil {
					t.Errorf("k:%d,m:%d fails when fileSize is %d, for %s", k, m, fileSize, err.Error())
				}
			}
		}
		f.Close()
		err = os.Remove(tempFile)
		if err != nil {
			t.Fatal(err)
		}
	}

}
