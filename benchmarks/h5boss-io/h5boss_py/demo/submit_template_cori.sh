#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 1
#SBATCH -t 00:05:00
#SBATCH -J template-mpi
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -L SCRATCH
##SBATCH --mail-type=END
##SBATCH --mail-user=jalnliu@lbl.gov
cd $SLURM_SUBMIT_DIR

# Control Arguments:
libver="libver="$1
cmdscript="../scripts/test_file_creation_cori.py "
nproc="1"
cmd="srun -n "$nproc" python-mpi "$cmdscript

# Positional Arguments:
pkinput=" global_fiber1k "
h5template=$CSCRATCH/bosslover/scaling-test/ost2/$SLURM_JOB_ID.h5
version=" "$1" " #earliest
allotime=" "$3" " #late
driver=" "$2" " #core, sec2, stdio, 

run=$cmd$pkinput$h5template$version$allotime$driver
echo $run
echo $libver
$run
