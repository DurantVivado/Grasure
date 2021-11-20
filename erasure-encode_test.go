// This test unit tests the encoding and decoding efficiency
//
package main

import (
	"crypto/rand"
	"io"
	"testing"
)

//randomly generate file of different size and encode them into HDR system
const KiB = 1024
const MiB = 1048576

type fileUnit struct {
	fileSize int64
	fileHash string
}

var erasureEncodeDecodeTests = []struct {
	dataBlocks, parityBlocks   int
	blockSize                  int64
	missingData, missingParity int
	reconstructParity          bool
	shouldFail                 bool
}{
	//Block size set to 4KiB
	{dataBlocks: 2, parityBlocks: 2, blockSize: 4 * KiB, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, blockSize: 4 * KiB, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, blockSize: 4 * KiB, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, blockSize: 4 * KiB, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, blockSize: 4 * KiB, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, blockSize: 4 * KiB, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, blockSize: 4 * KiB, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, blockSize: 4 * KiB, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, blockSize: 4 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, blockSize: 4 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},
	//Block size set to 128KiB
	{dataBlocks: 2, parityBlocks: 2, blockSize: 128 * KiB, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, blockSize: 128 * KiB, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, blockSize: 128 * KiB, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, blockSize: 128 * KiB, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, blockSize: 128 * KiB, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, blockSize: 128 * KiB, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, blockSize: 128 * KiB, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, blockSize: 128 * KiB, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, blockSize: 128 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, blockSize: 128 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},
	//Block size set to 256KiB
	{dataBlocks: 2, parityBlocks: 2, blockSize: 256 * KiB, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, blockSize: 256 * KiB, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, blockSize: 256 * KiB, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, blockSize: 256 * KiB, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, blockSize: 256 * KiB, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, blockSize: 256 * KiB, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, blockSize: 256 * KiB, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, blockSize: 256 * KiB, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, blockSize: 256 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, blockSize: 256 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},

	//Block size set to 512KiB
	{dataBlocks: 2, parityBlocks: 2, blockSize: 512 * KiB, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, blockSize: 512 * KiB, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, blockSize: 512 * KiB, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, blockSize: 512 * KiB, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, blockSize: 512 * KiB, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, blockSize: 512 * KiB, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, blockSize: 512 * KiB, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, blockSize: 512 * KiB, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, blockSize: 512 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, blockSize: 512 * KiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},
	//Block size set to 1MiB
	{dataBlocks: 2, parityBlocks: 2, blockSize: MiB, missingData: 0, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 3, parityBlocks: 3, blockSize: MiB, missingData: 1, missingParity: 0, reconstructParity: true, shouldFail: false},
	{dataBlocks: 4, parityBlocks: 4, blockSize: MiB, missingData: 2, missingParity: 0, reconstructParity: false, shouldFail: false},
	{dataBlocks: 5, parityBlocks: 5, blockSize: MiB, missingData: 0, missingParity: 1, reconstructParity: true, shouldFail: false},
	{dataBlocks: 6, parityBlocks: 6, blockSize: MiB, missingData: 0, missingParity: 2, reconstructParity: true, shouldFail: false},
	{dataBlocks: 7, parityBlocks: 7, blockSize: MiB, missingData: 1, missingParity: 1, reconstructParity: false, shouldFail: false},
	{dataBlocks: 8, parityBlocks: 8, blockSize: MiB, missingData: 3, missingParity: 2, reconstructParity: false, shouldFail: false},
	{dataBlocks: 2, parityBlocks: 2, blockSize: MiB, missingData: 2, missingParity: 1, reconstructParity: true, shouldFail: true},
	{dataBlocks: 4, parityBlocks: 2, blockSize: MiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: true},
	{dataBlocks: 8, parityBlocks: 4, blockSize: MiB, missingData: 2, missingParity: 2, reconstructParity: false, shouldFail: false},
}

func TestErasureEncode(t *testing.T) {
	data := make([]byte, 256)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		t.Logf("Failed to read random data:%v", err)
	}
	//we encode and decode the randomly generated data bytes
	for i, test := range erasureEncodeDecodeTests {
		buffer := make([]byte, len(data), 2*len(data))
		copy(data, buffer)
		e := Erasure{k: test.dataBlocks, m: test.parityBlocks, blockSize: int(test.blockSize)}
		//we make random data and encode into parity
		e.Encode()

	}
}
