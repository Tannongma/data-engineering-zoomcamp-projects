#! /bin/bash
apt update


apt install anaconda -y
source /usr/bin/conda init bash
source ~/.bashrc

conda create -n dev_env python=3.9 -y
source ~/.bashrc
conda activate dev_env

pip install --upgrade pip
