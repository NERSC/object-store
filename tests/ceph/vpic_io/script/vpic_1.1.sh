#!/bin/bash 
#This is single node multiple processes scaling tests for vpic IO on ceph/rados
for j in 1 2 3 # repeat three times
do 
  #for i in 1 2 4 8 16 32  # testing 1 to 32 processes
  for i in 32 
   do
        echo "Test:$j Proc:$i" 
	#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
	#default write size is 8 million particles, which is 256 MB, we used 2*8 million, =512MB
	#non-chunking by default
        srun -n $i ./VPIC vr${j}p${i}.h5 swiftpool $CEPH_CONF 262144 0 >> log.2 # 262144 is 8MB data
	sleep 1
	echo "submited job "$i
	#clean the data
   done
done
