#!/bin/bash
#SBATCH -p regular
#SBATCH -N 102
#SBATCH -t 01:50:00
#SBATCH -J boss2hdf5-parallel
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=ALL

cd $SLURM_SUBMIT_DIR
srun -n 2448 python-mpi boss2hdf5-parallel.py 
