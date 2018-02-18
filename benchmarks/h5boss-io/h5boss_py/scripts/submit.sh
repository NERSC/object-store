#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 19
#SBATCH -t 00:05:00
#SBATCH -J random-pmf
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -V

cd $SLURM_SUBMIT_DIR
srun -n 600 python-mpi random-pmf.py ../demo/input_csv/input-full-cori_v1 

