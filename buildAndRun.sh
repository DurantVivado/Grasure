#!/bin/bash
file=Goprogramming.pdf
go build -o grasure erasure-*.go main.go
# init the system
# ./grasure -md init -k 12 -m 4 -bs 4096

# A loopback system
./grasure -md encode -f test/$file -conStripes 100 -o
./grasure -md read -f $file -fn 1 -conStripes 100 -sp output/$file
srchash=(`sha256sum test/$file|tr ' ' ' '`)
dsthash=(`sha256sum output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
