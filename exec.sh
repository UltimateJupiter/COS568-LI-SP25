#!/bin/bash -l
#SBATCH --job-name=568Eval
#SBATCH --output=slurm_files/slurmout-%x.%j.out
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=100G
#SBATCH --time=2:00:00
#SBATCH --partition=pli-c

bash ./scripts/run_benchmarks.sh