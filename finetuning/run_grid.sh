#!/bin/bash
# Fine-tuning via nh_run continue_training + checkpoint_path
# Usage : bash finetuning/run_grid.sh

echo '>>> finetune_lr1e-5_ep3'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr1e-5_ep3.yml

echo '>>> finetune_lr1e-5_ep5'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr1e-5_ep5.yml

echo '>>> finetune_lr1e-5_ep10'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr1e-5_ep10.yml

echo '>>> finetune_lr5e-5_ep3'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr5e-5_ep3.yml

echo '>>> finetune_lr5e-5_ep5'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr5e-5_ep5.yml

echo '>>> finetune_lr5e-5_ep10'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr5e-5_ep10.yml

echo '>>> finetune_lr1e-4_ep3'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr1e-4_ep3.yml

echo '>>> finetune_lr1e-4_ep5'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr1e-4_ep5.yml

echo '>>> finetune_lr1e-4_ep10'
python -m neuralhydrology.nh_run continue_training --run-dir ./runs/arlstm_feat27jHigh_modele2_1805_111000 --config-file ./finetuning/yamls/finetune_lr1e-4_ep10.yml

