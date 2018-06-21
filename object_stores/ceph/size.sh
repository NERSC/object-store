#!/bin/bash
if [ "$#" -ne 3 ]; then
    echo "poolname objectnames output"
    exit
fi
while read p; do
  rados --user jialin -p $1 stat $p >> $3
done <$2
