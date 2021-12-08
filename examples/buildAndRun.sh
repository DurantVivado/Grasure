#!/bin/bash
file=Goprogramming.pdf
go build -o main ./main.go
# init the system
./main -md init -k 12 -m 4 -dn 20 -bs 4096

# to encode a file 
./main -md encode -f test/$file -conStripes 100 -o
# to update a file
# ./main -md update -f $file -nf test/$file
# to read a file
./main -md read -f $file -fmd bitRot -fn 2 -conStripes 100 -sp output/$file
srchash=(`sha256sum test/$file|tr ' ' ' '`)
dsthash=(`sha256sum output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
