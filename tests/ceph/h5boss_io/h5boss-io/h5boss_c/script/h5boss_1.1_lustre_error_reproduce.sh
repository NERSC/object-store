#!/bin/bash -l
#SBATCH -N 1 
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 30:00
#SBATCH -L SCRATCH
##SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
#Lustre Settings
Lustre_dir=$SCRATCH/h5boss_ost4
mkdir -p $Lustre_dir
rm -rf $Lustre_dir
mkdir -p $Lustre_dir
lfs setstripe -c 1 -S 4m $Lustre_dir
lfs getstripe $Lustre_dir

#Lustre Usage
#Usage: srun -n n_proc ./h5boss_lustre /path/to/dset_list.csv output_file n_dsets
input=/global/cscratch1/sd/jialin/dset_stats.csv
srun -n 1 ./h5boss_lustre $input $Lustre_dir/boss.hdf5 100000

