#!/bin/bash

for i in 100 200 400 800 1600 3200 
do
  #objsize=$(( i*1048576 ))
  for j in 1 2 3 4 5 
  do
  	./librados_test $CEPH_CONF stripe_file_test_${j} ${i} 1 16 16777216 1048576 >> filesize.test
  done
done
