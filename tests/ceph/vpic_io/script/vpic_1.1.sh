#!bin/bash 
#This is single node multiple processes scaling tests for vpic IO on ceph/rados
for j in 1 2 3 # repeat three times
do 
  for i in { 1..32}  # testing 1 to 32 processes
   do
        echo "Test:$j Proc:$i" >> vpic_1.1.log 
	#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
	#default write size is 8 million particles, which is 256 MB, we used 2*8 million, =512MB
	#non-chunking by default
        srun -n $i ./VPIC vpic_test1.1_repeat${j}_process${i}.h5 swiftpool $CEPH_CONF 16 0 >> vpic_1.1.log
	#clean the data
   done
done
