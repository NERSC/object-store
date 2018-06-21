#!/bin/bash

for i in 1 4 8 16 32 64 128 
do
  objsize=$(( i*1048576 ))
  for j in 1 2 3 4 5 
  do
  	./librados_test $CEPH_CONF stripe_obj_test_${j} 10 20 4 ${objsize}  1048576 >>objsize.test
  done
done
