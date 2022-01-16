#!/bin/bash
file=testfile.txt
go build -o main ./main.go
# init the system
./main -md init -k 12 -m 4 -dn 16 -bs 4096

# to encode a file 
./main -md encode -f ~/input/$file -conStripes 100 -o
# to update a file
# ./main -md update -f $file -nf test/$file
# to read a file
# ./main -md read -f $file -conStripes 100 -sp ~/output/$file
# to remove a file
# ./main -md delete -f $file
# to recover a disk
# ./main -md recover -fmd diskFail -fn 1
# ./main -md rws -fmd diskFail -fn 1 -f ~/input/$file

# srchash=(`sha256sum ~/input/$file|tr ' ' ' '`)
# dsthash=(`sha256sum ~/output/$file|tr ' ' ' '`)
# echo $srchash
# echo $dsthash
# if [ $srchash == $dsthash ];then 
#     echo "hash check succeeds"
# else
#     echo "hash check fails"
# fi
