# Grasure

Simplified Erasure Coding Architecture in Go
Implementing most popular erasured-based filesystem operations, it's readily used and integrated into other filesystems. 
Project home: https://github.com/DurantVivado/Grasure
Godoc: -


## Project Architecture:
- `main.go` contains the main func. For each run,  operate among "encode", "read", "update", "scaling", "delete", ...

- `erasure-global.go` contains the system-level interfaces and global structs and variables

- `erasure-init.go` contains the basic config file(`.hdr.sys`) read and write operation, once there is file change, we update the config file.

- `erasure-errors.go` contains the definitions for various possible errors.

- `erasure-encode.go` contains operation for striped file encoding, one great thing is that you could specify the data layout. 

- `erasure-encode.go` contains operation for striped file encoding, you could specific the layout. 

- `erasure-read.go` contains operation for striped file reading, if some parts are lost, we try to recover.

import:
[reedsolomon library](https://github.com/klauspost/reedsolomon)


## Usage
0. Build the project:
```
go build -o grasure erasure-*.go main.go
```
1. New a file named `.hdr.disks.path` in project root, type the path of your local disks, e.g.,
```
/home/server1/data/data1
/home/server1/data/data2
/home/server1/data/data3
/home/server1/data/data4
/home/server1/data/data5
/home/server1/data/data6
/home/server1/data/data7
/home/server1/data/data8
/home/server1/data/data9
/home/server1/data/data10
/home/server1/data/data11
/home/server1/data/data12
/home/server1/data/data13
/home/server1/data/data14
/home/server1/data/data15
/home/server1/data/data16
```
2.Initialise the system, you should explictly attach the number of data(k) and parity shards (m) as well as blocksize (in bytes), remember k+m must NOT be bigger than 256.
```
./grasure -md init -k 12 -m 4 -bs 4096
```
3. Encode one examplar file.
```
./grasure -md encode -f {source file path} -conStripes 100 -o
```

4. Decode(read) the examplar file.
```
./grasure -md read -f {source file basename} -conStripes 100 -sp {destination file path} -fn {fail disk number}
```
here `conStripes` denotes how many stripes are allowed to operate concurrently, default value is 100. 
`sp` means save path.

use `fn` to simulate the failed number of disks (default is 0), for example, `-fn 2` simluates shutdown of arbitrary two disks. Relax, the data will not be really lost.

5. check the hash string to see encode/decode is correct.

```
sha256sum {source file path}
```
```
sha256sum {destination file path}
```


## CLI parameters

|parameter(alias)|description|default|
|--|--|--|
|blockSize(bs)|the block size in bytes|4096|
|mode(md)|the mode of ec system, one of (encode, decode, update, scaling, recover)||
|dataNum(k)|the number of data shards|12|
|parityNum(m)|the number of parity shards(fault tolerance)|4|
|filePath(f)|upload: the local file path, download&update: the remote file basename||
|savePath|the local save path (local path)|file.save|
|newDataNum(new_k)|the new number of data shards|32|
|newParityNum(new_m)|the new number of parity shards|8|
|recoveredDiskPath(rDP)|the data path for recovered disk, default to /tmp/restore| /tmp/restore|
|override(o)|whether to override former files or directories, default to false|false|
|conWrites(cw)|whether to enable concurrent write, default is false|false|
|conReads(cr)|whether to enable concurrent read, default is false|false|
|failMode(fmd)|simulate [diskFail] or [bitRot] mode"|diskFail|
|failNum(fn)|simulate multiple disk failure, provides the fail number of disks|0|
|conStripes(cs)|how many stripes are allowed to encode/decode concurrently|100|

## Performance


## Contributions