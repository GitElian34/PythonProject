import torch
import numpy as np
import random

SEED = 42
torch.manual_seed(SEED)
torch.cuda.manual_seed(SEED)
np.random.seed(SEED)
random.seed(SEED)
torch.backends.cudnn.deterministic = True
torch.set_num_threads(10)

from pathlib import Path
from neuralhydrology.nh_run import start_run

start_run(config_file=Path("./AI/LSTM/NeuralHydro/config.yaml"))