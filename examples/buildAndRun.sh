#!/bin/bash
file=testfile.txt
go build -o main ./main.go

if [ -f ".hdr.disks.path.old" ]; then
    rm -rf .hdr.disks.path  
    mv .hdr.disks.path.old .hdr.disks.path
fi

# init the system

./main -md init -k 4 -m 2 -dn 6 -bs 1

# to encode a file 
./main -md encode -f ~/input/$file -conStripes 100 -o

# to recover a disk
./main -md recover -fmd diskFail -fn 1 -conStripes 100
# ./main -md rws -fmd diskFail -fn 1 -f ~/input/$file -conStripes 100

# to read a file
./main -md read -f $file -conStripes 100 -sp ~/output/$file

srchash=(`sha256sum ~/input/$file|tr ' ' ' '`)
dsthash=(`sha256sum ~/output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi

# rm -rf ~/output/$file

# to update a file
# ./main -md update -f $file -nf test/$file

# to remove a file
# ./main -md delete -f $file
# ./main -md recover -fmd diskFail -fn 1




