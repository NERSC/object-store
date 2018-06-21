#!/bin/bash
for i in `seq 1 48`;
do 
	ceph osd deep-scrub $i
done
