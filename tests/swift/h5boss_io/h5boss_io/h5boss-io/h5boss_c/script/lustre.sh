#!/bin/bash -l
#SBATCH -N 2
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 30:00
#SBATCH -L SCRATCH
mkdir -p $SCRATCH/h5boss_ost24
stripe_medium $SCRATCH/h5boss_ost24
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets(optional)\n
srun -n 64 ./h5bossio dset_stats.csv $SCRATCH/h5boss_ost24/bossio.hdf5 10000
