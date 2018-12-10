#!/bin/bash -l
#SBATCH -N 32 
#SBATCH -q debug
#SBATCH -C haswell
#SBATCH -t 30:00
#SBATCH -L SCRATCH
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=END
#SBATCH -A nstaff
#Lustre Settings
Lustre_dir=$SCRATCH/h5boss_ost4
mkdir -p $Lustre_dir
rm -rf $Lustre_dir
mkdir -p $Lustre_dir
lfs setstripe -c 1 -S 4m $Lustre_dir
lfs getstripe $Lustre_dir

#Lustre Usage
#Usage: srun -n n_proc ./h5boss_write_dummy_data /path/to/dset_list.txt output_file n_dsets(optional)
echo 'lustre test'
#for k in 128 1024 10240 #100000 #1000000 # number of datasets
#do
s=8
for i in 1 #2 3 # number of repeat tests
 do
  for j in 32 64 128 256 #512 1024 # number of processes
  do 
   k=$((s*j))
   echo "submiting "$i"-th job to lustre:"$k" datasets "$j" processes"
   srun -n $j ./h5boss_lustre dset_stats.csv $Lustre_dir/boss_${k}_${i}_${j}.hdf5 $k 
   wait 
  done
done
#done

