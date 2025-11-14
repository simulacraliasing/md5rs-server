#!/bin/bash
cd /app

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu:/app

./md5rs-server -m "${MD_MODEL:-models/md_v5a_d_pp_fp16.onnx}" -d 0 --detect-workers "${WORKERS:-2}" --log-level "${LOG_LEVEL:-info}"
