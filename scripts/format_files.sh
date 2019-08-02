#!/bin/bash

pwd="$( cd "$( dirname "$0"  )" && pwd )"
root_dir="$( cd $pwd/.. && pwd )"
cd $root_dir

linenoise=./src/shell/linenoise
sds=./src/shell/sds

if [ $# -eq 0 ]; then
  echo "formating all .h/.cpp files in $root_dir ..."
  find . -type f -not \( -wholename "$linenoise/*" -o -wholename "$sds/*" -o -wholename "./rocksdb/*" -o -wholename "./rdsn/*" \) \
      -regextype posix-egrep -regex ".*\.(cpp|h)" | xargs clang-format -i -style=file
elif [ $1 = "-h" ]; then
  echo "USAGE: ./format-files.sh [<relative_path>] -- format .h/.cpp files in $root_dir/relative_path"
  echo "       ./format-files.sh means format all .h/.cpp files in $root_dir"
else
  echo "formating all .h/.cpp files in $root_dir/$1 ..."
  find ./$1 -type f -not \( -wholename "$linenoise/*" -o -wholename "$sds/*" -o -wholename "./rocksdb/*" -o -wholename "./rdsn/*" \) \
      -regextype posix-egrep -regex ".*\.(cpp|h)" | xargs clang-format -i -style=file
fi

