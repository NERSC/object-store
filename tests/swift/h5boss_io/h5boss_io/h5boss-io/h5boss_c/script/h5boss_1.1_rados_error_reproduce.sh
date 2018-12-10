#!/bin/bash -l
#SBATCH -N 1
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 30:00
##SBATCH -L SCRATCH
##SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
#Rados Settings
export CEPH_CONF=/global/cscratch1/sd/jialin/ceph_conf/ceph.conf
export POOL=swiftpool
#Rados Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets ceph_conf poolname

srun -n 8 ./h5boss_rados /global/cscratch1/sd/jialin/dset_stats.csv boss_rados.h5 100 $CEPH_CONF swiftpool
