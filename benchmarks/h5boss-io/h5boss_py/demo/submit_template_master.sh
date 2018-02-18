#!/bin/bash

for i in 1 2 3
do
 sbatch submit_template.sh latest None
 sbatch submit_template.sh earliest None
 sbatch submit_template.sh latest sec2
 sbatch submit_template.sh latest stdio
 sbatch submit_template.sh latest core
done

