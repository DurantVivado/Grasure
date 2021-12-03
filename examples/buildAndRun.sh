#!/bin/bash
file=Goprogramming.pdf
go build -o main ./main.go ./flag.go 
# init the system
./main -md init -k 12 -m 4 -dn 24 -bs 1048576

# A loopback system
# ./main -md encode -f test/$file -conStripes 100 -o
# ./main -md read -f $file -fn 0 -conStripes 100 -sp output/$file
srchash=(`sha256sum test/$file|tr ' ' ' '`)
dsthash=(`sha256sum output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
