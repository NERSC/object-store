#!/bin/bash
#SBATCH -p regular
#SBATCH -N 1
#SBATCH -t 00:10:00
#SBATCH -J subset-serial
#SBATCH -e %j_1k.err
#SBATCH -o %j_1k.out
cd $SLURM_SUBMIT_DIR
SCRATCH_A=/project/projectdirs/mpccc/jialin
TESTDIR=$SCRATCH/bosslover/scaling-test/
rm $TESTDIR/1k_cmp_edison.h5 >/dev/null
subset input_csv/input-full $TESTDIR/1k_cmp_edison.h5 pmf-list/pmf1k
