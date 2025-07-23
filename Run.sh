source $(conda info --base)/etc/profile.d/conda.sh
conda activate HumanDatabaseTools
nohup python pipeline_worker.py &