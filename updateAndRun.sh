#!/bin/bash
file=LICENSE
go build -o grasure erasure-*.go main.go
# init the system
# sudo ./grasure -md init -k 2 -m 2 -bs 4096

# A loopback system
sudo ./grasure -md update -f $file
sudo ./grasure -md read -f $file -fn 0 -conStripes 100 -sp ~/output/$file
srchash=(`sha256sum $file|tr ' ' ' '`)
dsthash=(`sha256sum ~/output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
