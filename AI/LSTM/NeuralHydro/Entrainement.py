import torch
torch.set_num_threads(30)

from pathlib import Path
from neuralhydrology.nh_run import start_run

start_run(config_file=Path("./AI/LSTM/NeuralHydro/config.yaml"))