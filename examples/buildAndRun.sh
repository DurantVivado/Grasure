#!/bin/bash
filename=test-64Mx32
inputdir=~/input/
outputdir=~/output/
newfilename=new-64Mx16
#data shards
k=6
#parity shards
m=2
#used disk number
dn=32
#block size
bs=1048576
#memory limit
mem=8
#failed disk number
fn=1
#specified failed disk, starting from 0, use comma to split
fd=0
# 4k 4096
# 1M 1048576
# 4M 4194304
# 16M 16777216
# 64M 67108864
# 128M 134217728
# 256M 268435456

go build -o main ./main.go
start=`date +%s%N`
now=`date +%c` 
echo "The program starts at $now."  
#------------------------encode a file--------------------------
# init the system
# ./main -md init -k $k -m $m -dn $dn -bs $bs -mem $mem

# to encode a file 
# ./main -md encode -f $inputdir$filename -conStripes 100 -o


# to update a file
# ./main -md update -f $filename -nf $newfilename
# to read a file
# ./main -md read -f $filename -conStripes 100 -sp $outputdir$filename
# to remove a file
# ./main -md delete -f $filename

# srchash=(`sha256sum $inputdir$filename|tr ' ' ' '`)
# dsthash=(`sha256sum $outputdir$filename|tr ' ' ' '`)
# echo $srchash
# echo $dsthash
# if [ $srchash == $dsthash ];then 
#     echo "hash check succeeds"
# else
#     echo "hash check fails"
# fi

#---------------------------repair the file----------------------
# recover a file
# methods: baseline, sga, gca
./main -md baseline -fmd diskFail -fd $fd -f $inputdir$filename -conStripes 100 -o
end=`date +%s%N`
cost=`echo $start $end | awk '{ printf("%.3f", ($2-$1)/1000000000) }'`
echo "sh: previous procedure consumed $cost s"