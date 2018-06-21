#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 1 
#SBATCH -t 00:10:00
#SBATCH -J librados
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -C haswell
#SBATCH --mail-type=END
#SBATCH --mail-user=jalnliu@lbl.gov

srun -n 16 ./librados_test $CEPH_CONF test 8 10 4 4194304 4194304
