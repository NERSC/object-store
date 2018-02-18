#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 1 
#SBATCH -t 00:20:00
#SBATCH -J subset-mpi
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -L SCRATCH
#SBATCH -C haswell
#SBATCH --ntasks-per-node=32
cd $SLURM_SUBMIT_DIR
# below is the added line. Must be after the module command!
# source /project/projectdirs/m779/python-mpi/nersc/activate.sh

# Control Arguments:
version="_v1" # or "_v2"
cmdscript="../scripts/subset_mpi"$version".py "
nproc="32"
#cmd="srun -n "$nproc" python-mpi -m memory_profiler "$cmdscript
cmd="srun -n "$nproc" python-mpi "$cmdscript

# Positional Arguments:
srcfile=" input_csv/input-full-cori"$version" "
template=$CSCRATCH/bosslover/scaling-test/ost1/$SLURM_JOB_ID.h5
randpmf=$SLURM_JOB_ID.txt
npmf=10000
#shuf -n $npmf pmf-list/large-scale/pmf500k -o pmf-list/large-scale/$randpmf
#sed -i '1i\'"plates mjds fibers" pmf-list/large-scale/$randpmf
#pmfquery=" pmf-list/large-scale/"$randpmf" "
pmfquery=" pmflist/10k_nov2 "
# Optional Arguments:
k_opt1=" --mpi="         # 'yes' for parallel read/wirte 
                         # 'no'  for serial read/write

k_opt2=" --template="    # 'yes' for creating a template only 
 		         # 'no'  for using previous template and writing the actual data into it
		         # 'all' for creating a template and writing the actual data into it

k_opt3=" --fiber="       # specify a file that could store the accessed fiber information
k_opt4=" --catalog="     # specify a file that could store the accssed catalog information

k_opt5=" --datamapr="    # specify a file that stored all fiber information of source files
                         # if not specified, will scan all source files to create a new datamap

k_opt6=" --datamapw="    # specify a file that could store the queried plate/mjd/fiber metadata info, to avoid communication cost. 

k_opt7=" --rwmode="      # specify the read/write mode, let h5boss do r: "read-only" or rw: "read&write". (r is a must in any cases)

v_opt1="yes"
v_opt2="all"
v_opt3=$SLURM_JOB_ID"_fiber.txt "
v_opt4=$SLURM_JOB_ID"_catalog.txt "
v_opt5="/global/cscratch1/sd/jialin/h5boss/map/allmap/merge_gcpk1.0"
v_opt6=$SLURM_JOB_ID".dmp"
v_opt7="r"

#Recipes 

#For generating a tiny pickle from the total-pickle file to avoid communication: 
#v_opt2="no " # use
#v_opt7="r"
#run=$cmd$srcfile$template$pmfquery$k_opt1$v_opt1$k_opt2$v_opt2$k_opt5$v_opt5$k_opt6$v_opt6$k_opt7$v_opt7

#For generate a tiny pickle from source files.
v_opt2="no"
v_opt7="r"
run=$cmd$srcfile$template$pmfquery$k_opt1$v_opt1$k_opt2$v_opt2$k_opt3$v_opt3$k_opt4$v_opt4$k_opt6$v_opt6$k_opt7$v_opt7
#run=$cmd$srcfile$template$pmfquery$k_opt1$v_opt1$k_opt2$v_opt2$k_opt3$v_opt3$k_opt4$v_opt4
echo $run >>${SLURM_JOB_ID}.err
echo $run
$run
