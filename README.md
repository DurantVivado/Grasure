[![Go Reference](https://pkg.go.dev/badge/github.com/DurantVivado/Grasure.svg)](https://pkg.go.dev/github.com/DurantVivado/Grasure) 
# Grasure

Universal Erasure Coding Architecture in Go
Implementing most popular erasured-based filesystem operations, it's readily used and integrated into other filesystems. 

Project home: https://github.com/DurantVivado/Grasure

Godoc: https://pkg.go.dev/github.com/DurantVivado/Grasure


## Project Architecture:
<!-- - `main.go` contains the main func. For each run,  operate among "encode", "read", "update", "scaling", "delete", ... -->

- `erasure-global.go` contains the system-level interfaces and global structs and variables

- `erasure-init.go` contains the basic config file(`.hdr.sys`) read and write operation, once there is file change, we update the config file.

- `erasure-errors.go` contains the definitions for various possible errors.

- `erasure-encode.go` contains operation for striped file encoding, one great thing is that you could specify the data layout. 

- `erasure-layout.go` You could specific the layout, for example, random data distribution or some other heuristics. 

- `erasure-read.go` contains operation for striped file reading, if some parts are lost, we try to recover.

- `erasure-update.go` contains operation for striped file updating, if some parts are lost, we try to recover.

- `erasure-recover.go` deals with multi-disk recovery, concerning both data and meta data.

- `erasure-update.go` contains operation for striped file updating, if some parts are lost, we try to recover first.

import:
[reedsolomon library](https://github.com/klauspost/reedsolomon)


## Usage
A complete demonstration of various CLI usage lies in `examples/buildAndRun.sh`. You may have a glimpse.
Here we elaborate the steps as following, in dir `./examples`:

0. Build the project:
```
go build -o main ./main.go ./flag.go 
```

1. New a file named `.hdr.disks.path` in `./examples`, list the path of your local disks, e.g.,
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

2. Initialise the system, you should explictly attach the number of data(k) and parity shards (m) as well as blocksize (in bytes), remember k+m must NOT be bigger than 256.
```
./main -md init -k 12 -m 4 -bs 4096 -dn 16
```
`bs` is the blockSize in bytes and `dn` is the diskNum you intend to use in `.hdr.disks.path`. Obviously, you should spare some disks for fault torlerance purpose.

3. Encode one examplar file.
```
./main -md encode -f {source file path} -conStripes 100 -o
```

4. decode(read) the examplar file.
```
./grasure -md read -f {source file basename} -conStripes 100 -sp {destination file path} 
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

6. To delete the file in storage (currently irreversible, we are working on that):
```
./main -md delete -f {filebasename} -o
```

7. To update a file in the storage:
```
./main -md update -f {filebasename} -nf {local newfile path} -o
```

8. Recover a disk(e.g. all the file blobs in failed disk(s)), and transfer it to backup disks. This turns to be time-consuming job. 
The previous disk path file will be renamed to `.hdr.disks.path.old`. New disk config path will replace every failed path with the redundant one.
```
./main -md recover 
```


6. update the examplar file.
```
./grasure -md update -f {source file path}
## Storage System Structure
We display the structure of storage system using `tree` command. As shown below, each `file` is encoded and split into `k`+`m` parts then saved in `N` disks. Every part named `BLOB` is placed into a folder with the same basename of `file`. And the system's metadata (e.g., filename, filesize, filehash and file distribution) is recorded in META. Concerning reliability, we replicate the `META` file K-fold.(K is uppercased and not equal to aforementioned `k`). It functions as the  general erasure-coding experiment settings and easily integrated into other systems.
It currently suppports `encode`, `read`, `update`, and more coming soon.
 ```
 server1@ubuntu:~/data$ sudo tree . -Rh
.
├── [4.0K]  data1
│   ├── [4.0K]  Goprogramming.pdf
│   │   └── [1.3M]  BLOB
│   └── [ 46K]  META
├── [4.0K]  data10
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data11
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data12
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data13
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data14
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data15
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data16
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data17
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data18
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data19
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data2
│   ├── [4.0K]  Goprogramming.pdf
│   │   └── [1.4M]  BLOB
│   └── [ 46K]  META
├── [4.0K]  data20
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.5M]  BLOB
├── [4.0K]  data21
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
├── [4.0K]  data22
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.3M]  BLOB
├── [4.0K]  data23
│   └── [4.0K]  Goprogramming.pdf
│       └── [1.4M]  BLOB
```


## CLI parameters
the command-line parameters of `./examples/main.go` are listed as below.
|parameter(alias)|description|default|
|--|--|--|
|blockSize(bs)|the block size in bytes|4096|
|mode(md)|the mode of ec system, one of (encode, decode, update, scaling, recover)||
|dataNum(k)|the number of data shards|12|
|parityNum(m)|the number of parity shards(fault tolerance)|4|
|diskNum(dn)|the number of disks (may be less than those listed in `.hdr.disk.path`)|4|
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
|quiet(q)|whether or not to mute outputs in terminal|false|
## Performance


## Contributions