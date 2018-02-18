#!/bin/bash
#SBATCH -p debug
#SBATCH -N 1 
#SBATCH -t 00:30:00
#SBATCH -J subset-mpi
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH --ntasks-per-node=32
##SBATCH --qos=premium
##SBATCH -A mpccc

cd $SLURM_SUBMIT_DIR
version="_v2" # or "_v2"
cmdscript="../scripts/subset_mpi"$version".py "
nproc="32"
cmd="srun -n "$nproc" python-mpi "$cmdscript
srcfile=" inputv2 "
pmf=10k_nov2
template=$CSCRATCH/h5boss_pre/${pmf}_v2_dec_30min1.h5
pmfquery=" pmflist/"$pmf" "
# Optional Arguments:
k_opt1=" --mpi="         # 'yes' for parallel read/wirte 
                         # 'no'  for serial read/write

k_opt2=" --template="    # 'yes' for creating a template only 
 		         # 'no'  for using previous template and writing the actual data into it
		         # 'all' for creating a template and writing the actual data into it
v_opt1="yes "
v_opt2="all "
k_opt3=" --datamap="
v_opt3="datamappk"

#run=$cmd$srcfile$template$pmfquery$k_opt1$v_opt1$k_opt2$v_opt2$k_opt3$v_opt3
run=$cmd$srcfile$template$pmfquery$k_opt1$v_opt1$k_opt2$v_opt2
echo $run
$run
