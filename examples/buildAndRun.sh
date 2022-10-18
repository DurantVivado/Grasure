#!/bin/bash
filename=test-64Mx320
inputdir=/mnt/disk15/
outputdir=/mnt/disk16/
newfilename=new-64Mx16
#data shards
k=6
#parity shards
m=2
#used disk number
dn=12
#block size
bs=67108864
#memory limit
mem=2
#failed disk number
fn=1
#specified failed disk, starting from 0, use comma to split
fd=1,2
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
echo -e "sh: The program starts at $now."  
#------------------------encode a file--------------------------
mode="none"
if [ $mode == "encode" ]; then
    # init the system
    ./main -md init -k $k -m $m -dn $dn -bs $bs -mem $mem

    # to encode a file 
    ./main -md encode -f $inputdir$filename -conStripes 100 -o


    # to update a file
    # ./main -md update -f $filename -nf $newfilename
    # to read a file
    ./main -md read -f $filename -conStripes 100 -sp $outputdir$filename
    # to remove a file
    # ./main -md delete -f $filename

    srchash=(`sha256sum $inputdir$filename|tr ' ' ' '`) #6cb118a8f8b3c19385874297e291dcbcdf3a9837ba1ca7b00ace2491adbff551
    dsthash=(`sha256sum $outputdir$filename|tr ' ' ' '`)
    echo -e "source file hash: $srchash"
    echo -e "target file hash: $dsthash"
    if [ $srchash == $dsthash ];then 
        echo -e "hash check succeeds"
    else
        echo -e "hash check fails"
    fi
else
#---------------------------repair the file----------------------
    # recover a file
    # methods: baseline, sga, gca-p, gca-f
    ./main -md gca-f -fmd diskFail -fd $fd -f $inputdir$filename -conStripes 100 -o
    end=`date +%s%N`
    cost=`echo $start $end | awk '{ printf("%.3f", ($2-$1)/1000000000) }'`
    echo -e "sh: previous procedure consumed $cost s"
fi