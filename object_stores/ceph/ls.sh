#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "./ls.sh poolname output"
    exit
fi
echo 'ls pool save all object names'
rados --user jialin -p $1 ls > $2

