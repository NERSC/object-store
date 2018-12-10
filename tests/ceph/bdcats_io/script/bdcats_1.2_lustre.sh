#!/bin/bash 
#This is single node weak scaling tests for bd-cats IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
##Default write size is 8 million particles, which is 256 MB

#Non-chunking by default
#SBATCH -N 32
#SBATCH -q debug 
#SBATCH -C haswell
#SBATCH -t 00:30:00
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
cd ../
source config.nersc
cd script
echo 'bdcats multi nodes weak scaling test on lustre, 8MB each process'
dir=$SCRATCH/vpic_lustre
for j in 1 2 3 # repeat three times
do 
  for i in 1 2 4 8 16 32  # testing 1 to 32 nodes
   do
        echo "Test:$j Nodes:$i" >> bdcats_1.2_lustre.log 
	nproc=32
        tproc=$((nproc*i))
	sizeperproc=262144 # 8mb
	nparticle=${sizeperproc}
	echo $nparticle' particles'
	echo $tproc 'nodes'
	filename=$dir/vpic_bdcats_nodes${i}_npar.h5
	poolname=swiftpool
	echo 'filename:'$filename
	srun -n $tproc  ./BDCATS_LUSTRE $filename $poolname $CEPH_CONF 262144 >> bdcats_1.2_lustre.log
	wait
	echo 'submitted job with '$i' processes'
   done
done
