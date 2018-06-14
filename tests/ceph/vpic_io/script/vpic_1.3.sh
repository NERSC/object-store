#!bin/bash 
#This is multiple nodes weak scaling tests for vpic IO on ceph/rados
#Workload per processes is remained same=8MB
#Parameters: file pool ceph.conf nparticles chunksize, if 0, then not do chunk write
#Default write size is 8 million particles, which is 256 MB, we used 2*8 million, =512MB
#Non-chunking by default
#Each node use 16 processes, which is half of the total ranks. 

date >>vpic_1.3.log
for j in 1 2 3 # repeat three times
do 
  for i in { 1..32 }  # testing 1 to 32 nodes
   do
        echo "Test:$j Proc:$i" >> vpic_1.3.log 
 	nproc=16
	tproc=$(( $nproc * $i ))
	sizeperproc=0.03125 # unit: million (particles)
	nparticle= $(( $tproc * $sizeperproc  ))
	filename=vpic_test1.2_1_repeat${j}_nodes${i}_proc${tproc}_npar${nparticle}.h5
	poolfile=swiftpool
        srun -n $tproc --ntasks-per-node=32 ./VPIC $filename $poolname $CEPH_CONF $nparticle 0 >> vpic_1.3.log
	#clean the data
   done
done
date >>vpic_1.3.log
