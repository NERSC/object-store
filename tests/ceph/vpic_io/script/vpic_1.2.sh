#!/bin/bash 
#This is multiple nodes weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Non-chunking by default
#Each node use 32 or 16 processes ---> 8 processes or 4 processes
#SBATCH -N 32
#SBATCH -q regular
#SBATCH -C haswell
#SBATCH -t 01:30:00
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
j=1
logfile=vpic_multinodes.log
nproc=8
sizerank=1048576 #32MB per rank
#do 
for i in 1 2 4 8 16 32 # testing 1 to 32 nodes
do
        echo "Node:$i" >> $logfile 
	tproc=$((nproc*i))
	echo $tproc 'processes'
	filename=vpic_${i}.h5
	poolname=swiftpool
	echo 'filename:'$filename
        srun -n $tproc  ./VPIC $filename $poolname $CEPH_CONF $sizerank 0 >> $logfile
	#clean the data
	echo 'submitted job with '$i' nodes'
done
#done
