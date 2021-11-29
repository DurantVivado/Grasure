#!/bin/bash
file=LICENSE
go build -o grasure erasure-*.go main.go
# init the system
sudo ./grasure -md init -k 12 -m 4 -bs 1

# A loopback system
sudo ./grasure -md encode -f $file -conStripes 100 -o
sudo ./grasure -md read -f $file -fn 0 -conStripes 100 -sp /home/grasure/output/$file
srchash=(`sha256sum $file|tr ' ' ' '`)
dsthash=(`sha256sum /home/grasure/output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
