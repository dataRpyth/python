#!/usr/bin/env bash

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# hard code to 3.6 temporarily
python_bin_path=$(which python3.6)

bash_profile_path=~/.bash_profile

source "$bash_profile_path"

cmd="$python_bin_path ${@:1}"

eval "${cmd}"