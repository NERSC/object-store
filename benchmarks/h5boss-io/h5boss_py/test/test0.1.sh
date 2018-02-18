#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 1 
#SBATCH -t 01:20:00
#SBATCH -J bossmiss 
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -L SCRATCH
#SBATCH -C haswell
##198
##136
#srun -n 8 python-mpi  miss_conver.py 128
srun -n 32 python-mpi  miss_conver.py 96
