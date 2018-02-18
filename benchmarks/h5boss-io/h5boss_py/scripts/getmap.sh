#!/bin/bash
#SBATCH -p regular 
#SBATCH -N 10
#SBATCH -t 00:16:00
#SBATCH -J h5map
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -A mpccc
#SBATCH -C haswell
cd $SLURM_SUBMIT_DIR
##2599

## srun -n 320 python-mpi get_all_fibermap1.py 0 allfiber_320
cmd="srun -n 320 python-mpi get_all_fibermap1.py 321 allfiber_641"
echo $cmd >>${SLURM_JOB_ID}.err
echo $cmd >>${SLURM_JOB_ID}.out
srun -n 320 python-mpi get_all_fibermap1.py 321 allfiber_641   # 321+320=641

#cmd="srun -n 320 python-mpi get_all_fibermap1.py 642 allfiber_962"
#echo $cmd >>${SLURM_JOB_ID}.err
#echo $cmd >>${SLURM_JOB_ID}.out
#srun -n 320 python-mpi get_all_fibermap1.py 642 allfiber_962   # 642+320=962



#cmd="srun -n 320 python-mpi get_all_fibermap1.py 963 allfiber_1283"
#echo $cmd >>${SLURM_JOB_ID}.err
#echo $cmd >>${SLURM_JOB_ID}.out
#srun -n 320 python-mpi get_all_fibermap1.py 963 allfiber_1283  # 963+320=1283
#cmd="srun -n 320 python-mpi get_all_fibermap1.py 1284 allfiber_1604"
#echo $cmd >>${SLURM_JOB_ID}.err
#echo $cmd >>${SLURM_JOB_ID}.out
#srun -n 320 python-mpi get_all_fibermap1.py 1284 allfiber_1604  # 1284+320=1604





#cmd="srun -n 320 python-mpi get_all_fibermap1.py 1605 allfiber_1925"
#echo $cmd >> %j.err
#echo $cmd >> %j.out
#srun -n 320 python-mpi get_all_fibermap1.py 1605 allfiber_1925 # 1605+320=1925
#cmd="srun -n 320 python-mpi get_all_fibermap1.py 1926 allfiber_2246"
#echo $cmd >> %j.err
#echo $cmd >> %j.out
#srun -n 320 python-mpi get_all_fibermap1.py 1926 allfiber_2246 # 1926+320=2246


## srun -n 355 python-mpi get_all_fibermap1.py 2244 allfiber_2599  # 2244+199=2244
