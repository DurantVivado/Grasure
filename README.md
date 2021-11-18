# Grasure
---
Simplified Erasure Coding Architecture in Go
Implementing most popular erasured-based filesystem operations, it's readily used and integrated into other filesystems. 
Project home: https://github.com/DurantVivado/Grasure
Godoc: -

We decide to base Grasure on Go-fuse for a native FUSE filesystem.

## Project Architecture:
- `main.go` contains the entering func, for each run, we could operate among "encode", "read", "update", "scaling", "delete", ...

- `erasure-global.go` contains the system-level interfaces and global structs and variables

- `erasure-init.go` contains the basic config file(`.hdr.sys`) read and write operation, once there is file change, we update the config file.

- `erasure-errors.go` contains the definitions for various possible errors.

- `erasure-encode.go` contains operation for striped file encoding, one great thing is that you could specify the data layout. 

- `erasure-encode.go` contains operation for striped file encoding, you could specific the layout. 

- `erasure-read.go` contains operation for striped file reading, if some parts are lost, we try to recover.

import:
[reedsolomon library](https://github.com/klauspost/reedsolomon)

---
