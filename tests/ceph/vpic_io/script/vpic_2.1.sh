#!bin/bash 
#This is multiple nodes strong scaling tests for vpic IO on ceph/rados
#Total workload size is 64 MB, this is to limit the size per proc less than 90 MB, rados io limit.
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#1-32 processes

tsize=64
usize=256
umill=8
logfile=vpic_2.1.log
particle=$((tsize/usize))
nparticle=$((particle*umill)) # e.g., 2048/256 * 8 = 64 million
poolfile=swiftpool
repeat=3
echo "Total Written size "$tsize" MB,">> $logfile
echo "Total number of particles:"$nparticle"million" >> $logfile
echo "Scaling from 1 to 32 processes on 1 Haswell node" >> $logfile

date >> $logfile

for j in { 1..$repeat } # repeat three times
do 
  for i in { 1..32}  # testing 1 to 32 processes
   do
        echo "Test:$j Proc:$i" >> $logfile
	filename=vpic_test2.1_repeat${j}_proc${i}_npar${nparticle}.h5
        srun -n $i  ./VPIC $filename $poolname $CEPH_CONF $nparticle 0 >> $logfile	
   done
done
date >> $logfile
