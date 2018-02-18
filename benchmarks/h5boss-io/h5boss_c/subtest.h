#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 2
#SBATCH -t 00:05:00
#SBATCH -J testh5g
#SBATCH -e %j.err
#SBATCH -o %j.out
srun -n 48 ./testh5g
