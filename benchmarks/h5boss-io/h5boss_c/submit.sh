#!/bin/bash
#SBATCH -p debug
#SBATCH -N 10 
#SBATCH -t 00:15:00
#SBATCH -J ch5boss
#SBATCH -e %j.err
#SBATCH -o %j.out
srun -n 300 ./subset.exe -f /global/cscratch1/sd/jialin/bosslover/scaling-test/ost72/1k_sep9.1058am.h5 -m 2970602_nodes1k_fiber.txt  -n 9619 -l 2970602_nodes1k_catalog.txt -k 981
