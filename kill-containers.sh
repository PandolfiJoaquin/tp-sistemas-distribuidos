#!/bin/bash

# Default mode if no parameter is provided (0=normal, 1=atomic-bomb, 2=deterministic)
MODE=${1:-0}

python3 el-fabuloso-cadillac.py --mode $MODE
