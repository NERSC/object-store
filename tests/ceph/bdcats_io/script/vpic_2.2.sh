#!bin/bash 
#This is multiple nodes strong scaling tests for vpic IO on ceph/rados
#Total workload size is 2048 MB, this is to limit the size per proc less than 90 MB, rados io limit.
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#32-1024 processes

tsize=2048
usize=256
umill=8
rank=32
repeat=3
chunksize=0 # default no chunking
particle=$((tsize/usize))
nparticle=$((particle*umill)) # e.g., 2048/256 * 8 = 64 million
poolfile=swiftpool
logfile=vpic_2.2.log
echo "Total Written size is "$tsize"MB">>$logfile
echo "Total number of particles is "$nparticle>>$logfile
echo "Scaling from 32 to 1024 processes on 1 to 32 nodes on Haswell" >>$logfile
date >>$logfile
for j in { 1..$repeat } # repeat three times
do 
  for i in { 32 64 128 256 512 1024 }  # testing 1 to 1024 processes
   do
        echo "Test:$j Proc:$i" >> $logfile 
	nodes=$(( $i / $rank ))
	filename=vpic_test2.2_repeat${j}_nodes${nodes}_proc${i}_npar${nparticle}.h5
	srun -n $i --ntasks-per-node=32 ./VPIC $filename $poolname $CEPH_CONF $nparticle $chunksize >> $logfile
   done
done
date >>$logfile
