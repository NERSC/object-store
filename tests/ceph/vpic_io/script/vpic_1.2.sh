#!/bin/bash 
#This is multiple nodes weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB
#Non-chunking by default
#Each node use 32 or 16 processes
##SBATCH -N 64
##SBATCH -q regular
##SBATCH -C haswell
##SBATCH -t 1:30:00
#cd ../
#source config.nersc
#cd script
for j in 1 2 3 # repeat three times
do 
  for i in 1 2 4 8 16 32 #64 # testing 1 to 32 nodes
   do
        echo "Test:$j Proc:$i" >> vpic_1.2.log.jun18_hello 
 	nproc=32
	tproc=$((nproc*i))
	#sizeperproc=0.03125 # unit: million (particles)
	#nparticle="$(echo "$tproc*$sizeperproc" | bc)"
	#nparticle=${nparticle%.*}
	#echo $nparticle' million particles'
	echo $tproc 'processes'
	filename=vpic_test1.2_repeat${j}_nodes${i}_proc${tproc}_npar.h5
	poolname=swiftpool
	#--ntasks-per-node=32
	echo 'filename:'$filename
        srun -n $tproc  ./VPIC $filename $poolname $CEPH_CONF 262144 0 >> vpic_1.2.log.june18_hello
	#clean the data
	echo 'submitted job with '$i' nodes'
   done
done
