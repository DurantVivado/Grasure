package grasure

import "path/filepath"

var testDiskFilePath = filepath.Join("examples", ".hdr.disks.path")

// randomly generate file of different size and encode them into HDR system
var dataShards = []int{
	2, 3, 4, 5, 6, 8, 9, 10, 12, 16, 18, 20,
}
var parityShards = []int{
	2, 3, 4,
}

var fileSizesV1 = []int64{
	128, 256, 512, 1024,
	128 * KiB, 256 * KiB, 512 * KiB,
	1 * MiB, 4 * MiB, 16 * MiB, 32 * MiB, 64 * MiB,
}
var fileSizesV2 = []int64{

	128 * MiB, 256 * MiB, 512 * MiB, 1024 * MiB,
}
var blockSizesV1 = []int64{
	4 * KiB, 16 * KiB, 64 * KiB,
	256 * KiB, 512 * KiB,
}

var blockSizesV2 = []int64{
	1 * MiB, 2 * MiB, 4 * MiB, 8 * MiB, 16 * MiB, 32 * MiB, 64 * MiB, 128 * MiB,
	256 * MiB,
}
