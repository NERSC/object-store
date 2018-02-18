#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 75
#SBATCH -t 00:30:00
#SBATCH -J boss2hdf_v2_part
#SBATCH -e %j_v2.err
#SBATCH -o %j_v2.out
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=ALL
cd $SLURM_SUBMIT_DIR
#module load python/2.7-anaconda
#module list
srun -n 2400 python-mpi boss2hdf5-parallel.py --output /global/cscratch1/sd/jialin/h5boss_v2/
