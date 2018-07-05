#!/bin/bash 
#This is single node weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#SBATCH -N 1
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 00:10:00
#SBATCH --mail-type=END
#SBATCH --mail-user=jalnliu@lbl.gov
dir=$SCRATCH/bdcats_lustre
mkdir -p $dir
lfs setstripe -c 1 -S 4m
#for j in 1 2 3 # repeat three times
j=1
#do 
for i in 1 2 4 8 16 32  # testing 1 to 32 processes
do
        echo "Test:$j Proc:$i" >> bdcats_vpic_1.1_lustre.log 
	tproc=$i
	sizeperproc=262144 # 0.25 million #0.03125 # unit: million (particles)
	#nparticle="$(echo "$tproc*$sizeperproc" | bc)"
	#nparticle=${nparticle%.*}
	nparticle=$sizeperproc
	echo $nparticle' million particles'
	echo $tproc 'processes'
	filename=$dir/bdcats_proc${tproc}_npar${nparticle}.h5
	poolname=swiftpool
	#--ntasks-per-node=32
	echo 'filename:'$filename
        srun -n $tproc  ./vpicio_lustre $filename $poolname $CEPH_CONF $nparticle 0 >> bdcats_vpic_1.1_lustre.log
	#clean the data
	wait
	echo 'submitted job with '$i' processes'
done
#done
