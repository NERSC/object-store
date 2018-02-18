#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 1 
#SBATCH -t 00:10:00
#SBATCH -J test_copy
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH --ntasks-per-node=32

#python test_h5copy.py $SCRATCH/h5boss/6703-56636.hdf5 /6703/56636/ 200 $SCRATCH/hdf-data/pm7.h5 1
#cp $SCRATCH/hdf-data/pm7.h5 $SCRATCH/hdf-data/pm7_value.h5

hostname

python test_h5copy.py $SCRATCH/hdf-data/pm7_value.h5  /6703/56636/ 200 $SCRATCH/hdf-data/pm7.h5 2
