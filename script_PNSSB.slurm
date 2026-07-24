#!/bin/bash
#
# Job name
#SBATCH --job-name=NeuralHydro
#
# jobs
#SBATCH --array=1
#
# Number of nodes
#SBATCH -N 1
#
# Number of tasks per node
#SBATCH -n 1
#
# CPUs per task (num_workers=4 + 1 pour le process principal)
#SBATCH --cpus-per-task=5
#
# Memory per CPU
#SBATCH --mem-per-cpu=5G
#
# Wall Time
#SBATCH --time=120:00:00
#
# Error log path
#SBATCH --error=/home/sar_hydro/STUDIES/EtudesEB/PythonProject/runs_SLURM/sbatch_error_%x.log
#
# Output log path
#SBATCH --output=/home/sar_hydro/STUDIES/EtudesEB/PythonProject/runs_SLURM/sbatch_output_%x.log
#
# Account
#SBATCH --account=all_users
#
# Mail address
#SBATCH --mail-user=eboaglio@groupcls.com
#
# Mail conditions
#SBATCH --mail-type=END,FAIL,TIME_LIMIT_90
#
echo "===== JOB INFO ====="
date
hostname
echo "SLURM_JOB_ID: $SLURM_JOB_ID"
echo "SLURM_JOB_NODELIST: $SLURM_JOB_NODELIST"
echo "CPUs visibles dans le job : $(nproc)"
echo "===================="

# Évite que les libs BLAS (numpy/torch) sursouscrivent les coeurs
# au-delà de ce que SLURM a réellement alloué
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export MKL_NUM_THREADS=$SLURM_CPUS_PER_TASK

source /home/sar_hydro/STUDIES/EtudesEB/PythonProject/.venv/bin/activate
cd /home/sar_hydro/STUDIES/EtudesEB/PythonProject || exit
python AI/LSTM/NeuralHydro/Entrainement.py