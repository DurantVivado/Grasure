#!/bin/bash
<<<<<<< HEAD
file=LICENSE
go build -o main ./main.go
# init the system
sudo ./main -md init -k 2 -m 2 -dn 24 -bs 1

# A loopback system
sudo ./main -md encode -f test/$file -conStripes 100 -o
sudo ./main -md read -f $file -fn 0 -conStripes 100 -sp output/$file
=======
file=Goprogramming.pdf
go build -o main ./main.go 
# init the system
# ./main -md init -k 12 -m 4 -dn 20 -bs 4096

# A loopback system
# encode objects
# ./main -md encode -f test/$file -conStripes 50 -o
# read objects
./main -md read -f $file -fn 0 -conStripes 100 -sp output/$file
# recover objects
# ./main md recover -fn 4 -o
>>>>>>> 9b1b0f47d88b6185915f25812e72ccf476c312f8
srchash=(`sha256sum test/$file|tr ' ' ' '`)
dsthash=(`sha256sum output/$file|tr ' ' ' '`)
echo $srchash
echo $dsthash
if [ $srchash == $dsthash ];then 
    echo "hash check succeeds"
else
    echo "hash check fails"
fi
