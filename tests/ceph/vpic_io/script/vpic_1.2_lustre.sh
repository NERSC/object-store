#!/bin/bash 
#This is multiple nodes weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#Each node use 32 or 16 processes
#SBATCH -N 32
#SBATCH -q regular
#SBATCH -C haswell
#SBATCH -t 01:30:00
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
#cd ../
#source config.nersc
#cd script
#for j in 1 2 3 # repeat three times
j=1
echo 'writing to lustre'
dir=$SCRATCH/vpic_lustre
rm -rf $dir
mkdir -p $dir
lfs setstripe -c 48 -S 4m $dir
#do 
logfile=vpic_lustre.log
nproc=8 
sizerank=1048576 #32MB per rank
for i in 4 4 # 8 16 32 #64 # testing 1 to 32 nodes
do
        echo "Node:$i" >> $logfile 
	tproc=$((nproc*i))
	echo $tproc 'processes'
	filename=$dir/vpic_lustre_nproc${tproc}.h5
	poolname=swiftpool
	echo 'filename:'$filename
        srun -n $tproc  ./vpicio_lustre $filename $poolname $CEPH_CONF $sizerank 0 >> $logfile
	#clean the data
	wait
	rm $filename
	echo 'submitted job with '$i' nodes'
done
#done
