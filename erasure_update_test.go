package grasure

import "testing"

type updateTest []struct {
	dataBlocks   int
	parityBlocks int
	diskNum      int
	blockSize    int64
	// missingData, missingParity int
	// reconstructParity          bool
	// shouldFail                 bool
}

func TestUpdate(t *testing.T) {
	// we generate temp data and encode it into real storage sytem
	// then change the file content randomly, and update it
	// after that, all temporary file should be deleted
	// fileSize:
	// Group1: 128, 256, 512 ,1024
	// Group2: 4k, 8k, 16k, 32k, ...,1024K
	// Group3: 1M, 2M, 4M, 8M, ...,1024M
	// Group4: 1G, 4G, 8G, 16G

}
