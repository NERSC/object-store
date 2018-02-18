#!/bin/bash
#SBATCH -p regular
#SBATCH -N 1
#SBATCH -t 01:05:00
#SBATCH -J check_fiber
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH -V

cd $SLURM_SUBMIT_DIR
python check_fiber.py
