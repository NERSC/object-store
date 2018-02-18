#!/bin/bash 
for i in `seq 1 10`;
do 
	echo id$i.h5
	cmd="subset_idx input_csv/input-full1 "$TESTDIR"/id"$i".h5 pmf-list/pmf1k1"
	echo $cmd
done
