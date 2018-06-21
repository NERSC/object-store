#!/bin/bash
Lustre_dir=$SCRATCH/h5boss_ost4
mkdir -p $Lustre_dir
rm -rf $Lustre_dir
mkdir -p $Lustre_dir
lfs setstripe -c 4 -s 4m $Lustre_dir
lfs getstripe $Lustre_dir

#Lustre Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets(optional)

#Rados Settings
export CEPH_CONF=/global/homes/j/jialin/object-store/object_stores/ceph/ceph_conf/ceph.conf
export POOL=swiftpool
#Rados Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets ceph_conf poolname

for k in 1000 10000 100000 1000000
do
 for i in 1 2 3
 do
  srun -n 1 ./h5boss_lustre dset_stats.csv $Lustre_dir/boss_${k}.hdf5 $k
  echo "submitted "$i"-th job to lustre:"$k" datasets"
  srun -n 1 ./h5boss_rados dset_stats.csv boss_${k}.h5 $k $CEPH_CONF swiftpool
  echo "submitted "$i"-th job to rados:"$k" datasets"
  wait
 done
done
