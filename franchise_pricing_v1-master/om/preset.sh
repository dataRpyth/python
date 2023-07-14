#!/usr/bin/env bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

week_of_month=$(echo $((($(date +%-d)-1)/7+1)))

python_bin_path=$(which python3.6)

# 当前月的第二个自然周周三下午18点，发送下三个自然月预埋价格表
if [[ "$week_of_month" == "2" ]]; then
  run_cmd="$script_dir/../bin/start.sh $script_dir/cron.py -preset -otaPricing"
  echo "run_cmd: $run_cmd"
  eval "$run_cmd"
fi