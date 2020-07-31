#!/bin/bash

if (($# < 2)); then
  echo "Two arguments required; source file and target file"
fi

source_file=$1
target_file=$2

python3 convert-datetimes.py -d 0 -t 3 4 <"$source_file" | sort -t , -k 1,2 >"$target_file"
rm "$source_file"