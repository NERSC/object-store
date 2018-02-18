#!/bin/bash
#SBATCH -p debug
#SBATCH -N 1 
#SBATCH -t 00:18:00
#SBATCH -J subset-v2-h5
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -L SCRATCH
#SBATCH -C haswell
##SBATCH --ntasks-per-node=32
cd $SLURM_SUBMIT_DIR

pmf=10k_nov2 #pmflist/10_nov2 100_nov2 1k_nov2 10k_nov2 100k_nov2 1m_nov2

#pre=/global/cscratch1/sd/jialin/h5boss_pre/${pmf}_v1.h5 
#pre=/global/cscratch1/sd/jialin/h5boss_pre/${pmf}_v2.h5
pre=none
#var=flux # WAVE
var=FLUX
ver=1
#case 1: WAVE and FLUX only
#TODO: rewrite the code to support case 2 and 3
	#case 2: WAVE, FLUX, IVAR, and MASK
	#case 3: all columns
#h5b test with pre-selected file, $SCRATCH/h5boss_pre/
cmd1="python test_h5bossread_pmf.py pmflist/"$pmf" "$var" "$pre" "$ver
#cmd1="mprof run test_h5bossread_pmf.py pmflist/"$pmf" "$var" "$pre

#h5b test with source files, $SCRATCH/h5boss
#cmd2="mprof run test_h5bossread_pmf.py pmflist/"$pmf" "$var" none"
cmd2="python test_h5bossread_pmf.py pmflist/"$pmf" "$var" none"
#fits test,/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0 
#cmd3="mprof run test_fitsread_pmf.py pmflist/"$pmf" flux"
cmd3="python test_fitsread_pmf.py pmflist/"$pmf" flux"
echo $cmd1 >> ${SLURM_JOB_ID}.out
echo $cmd1 >> ${SLURM_JOB_ID}.err
$cmd1
#echo $cmd2
#$cmd2
#echo $cmd3
#$cmd3

