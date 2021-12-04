#!/bin/bash
file=LICENSE
go build -o main ./main.go ./flag.go 
# init the system
sudo ./main -md init -k 12 -m 4 -dn 24 -bs 4096

# A loopback system
sudo ./main -md encode -f input/$file -conStripes 100 -o
sudo ./main -md read -f $file -fn 0 -conStripes 100 -sp output/$file
srchash=(`sha256sum input/$file|tr ' ' ' '`)
dsthash=(`sha256sum output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
