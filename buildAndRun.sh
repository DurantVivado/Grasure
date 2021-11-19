#!/bin/bash
go build -o grasure erasure-*.go main.go
# init the system
# ./grasure -md init -k 12 -m 4 -bs 4096

# A loopback system
./grasure -md encode -f test/Goprogramming.pdf -conStripes 100 -o
./grasure -md read -f Goprogramming.pdf -fn 3 -conStripes 100 -sp output/Goprogramming.pdf
srchash=(`sha256sum test/Goprogramming.pdf|tr ' ' ' '`)
dsthash=(`sha256sum output/Goprogramming.pdf|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi