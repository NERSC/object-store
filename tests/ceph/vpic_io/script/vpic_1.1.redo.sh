#!/bin/bash 
#This is single node weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#SBATCH -N 1
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 00:30:00
cd ../
source config.nersc
cd script
for j in 1 2 3 # repeat three times
do 
  for i in 1 2 4 8 16 32  # testing 1 to 32 processes
   do
        echo "Test:$j Proc:$i" >> vpic_1.1_redo.log 
	tproc=$i
	sizeperproc=0.03125 # unit: million (particles)
	nparticle="$(echo "$tproc*$sizeperproc" | bc)"
	#nparticle=${nparticle%.*}
	echo $nparticle' million particles'
	echo $tproc 'processes'
	filename=vpic_test1.2_repeat${j}_nodes${i}_proc${tproc}_npar${nparticle}.h5
	poolname=swiftpool
	#--ntasks-per-node=32
	echo 'filename:'$filename
        srun -n $tproc  ./VPIC $filename $poolname $CEPH_CONF $nparticle 0 >> vpic_1.1_redo.log
	#clean the data
	echo 'submitted job with '$i' processes'
   done
done
