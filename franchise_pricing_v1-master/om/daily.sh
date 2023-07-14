#!/usr/bin/env bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

day_of_week=$(date +%u)

python_bin_path=$(which python3.6)

# 当前月的第二个自然周周三下午18点，发送下三个自然月预埋价格表
if [[ "$day_of_week" == "1" || "$day_of_week" == "7" ]]; then
  echo "returning because it is Monday or Sunday"
  exit 0
fi

param1=${1:-}
param2=${2:-}
param3=${3:-}

run_cmd="$script_dir/../bin/start.sh $script_dir/cron.py $param1 $param2 $param3"
echo "run_cmd: $run_cmd"
eval "$run_cmd"