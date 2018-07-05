#!/bin/bash 
#This is single node weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#SBATCH -N 1
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 00:20:00
#SBATCH --mail-type=END
#SBATCH --mail-user=jalnliu@lbl.gov
j=1
#do
echo 'writing to rados' 
for i in 1 2 4 8 16 32  # testing 1 to 32 processes
do
        echo "Proc:$i" >> bdcats_vpic_1.1_redo.log 
	tproc=$i
	sizeperproc=262144 # 0.25 million, 8MB total
	nparticle=$sizeperproc
	echo $nparticle' particles'
	echo $tproc 'processes'
	filename=bdcats_proc${tproc}_npar${nparticle}.h5
	poolname=swiftpool
	echo 'filename:'$filename
        srun -n $tproc  ./VPIC $filename $poolname $CEPH_CONF 262144 0 >> bdcats_vpic_1.1_redo.log
	wait
	echo 'submitted job with '$i' processes'
done
#done
