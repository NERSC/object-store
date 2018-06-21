#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 1 
#SBATCH -t 01:10:00
#SBATCH -J vpic-rados
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -C haswell
#SBATCH --mail-type=END
#SBATCH --mail-user=jalnliu@lbl.gov

source config.nersc 
sbcast --compress=lz4 ./VPIC /tmp/VPIC

for i in 1 2 4 8 16 32 
do
   for j in  1 2 3
   do
	echo "procs:$i,test:$j"
 	srun -n $i ./VPIC vpic_p${i}.t${j}.h5 swiftpool $CEPH_CONF 8
	echo "done written vpic_p${i}.t${j}.h5"
   done
done
