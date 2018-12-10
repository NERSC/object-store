#!/bin/bash -l
#SBATCH -N 1
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 30:00
##SBATCH -L SCRATCH
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
#Rados Settings
export CEPH_CONF=/global/homes/j/jialin/object-store/object_stores/ceph/ceph_conf/ceph.conf
export POOL=swiftpool
#Rados Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets ceph_conf poolname

for k in 128 1024 10240 #100000 #1000000 # number of datasets
do
 for i in 1 2 3 # number of repeat tests
 do
  for j in 1 2 4 8 16 32 # number of processes
  do 
   echo "submiting "$i"-th job to rados:"$k" datasets "$j" processes"
   srun -n $j ./h5boss_rados dset_stats.csv boss_${k}_${i}_${j}.h5 $k $CEPH_CONF swiftpool  
   wait
  done
 done
done
