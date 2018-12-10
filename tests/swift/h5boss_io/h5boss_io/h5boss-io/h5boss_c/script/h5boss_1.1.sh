#!/bin/bash -l
#SBATCH -N 2
#SBATCH -q regular
#SBATCH -C haswell
#SBATCH -t 30:00
#SBATCH -L SCRATCH
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
#Lustre Settings
Lustre_dir=$SCRATCH/h5boss_ost4
mkdir -p $Lustre_dir
rm -rf $Lustre_dir
mkdir -p $Lustre_dir
lfs setstripe -c 1 -S 4m $Lustre_dir
lfs getstripe $Lustre_dir

#Lustre Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets(optional)

#Rados Settings
export CEPH_CONF=/global/homes/j/jialin/object-store/object_stores/ceph/ceph_conf/ceph.conf
export POOL=swiftpool
#Rados Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets ceph_conf poolname

for k in 100 1000 10000 #100000 #1000000 # number of datasets
do
 for i in 1 2 3 # number of repeat tests
 do
  for j in 1 2 4 8 16 32 # number of processes
  do 
   echo "submiting "$i"-th job to lustre:"$k" datasets "$j" processes"
   srun -n $j ./h5boss_lustre dset_stats.csv $Lustre_dir/boss_${k}_${i}_${j}.hdf5 $k &
   echo "submiting "$i"-th job to rados:"$k" datasets "$j" processes"
   srun -n $j ./h5boss_rados dset_stats.csv boss_${k}_${i}_${j}.h5 $k $CEPH_CONF swiftpool & 
  wait
  done
 done
done

