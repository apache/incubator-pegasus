#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
PROJECT_DIR=$(dirname $(dirname "${SCRIPT_DIR}"))
python ${SCRIPT_DIR}/run-clang-format.py \
	--clang-format-executable=clang-format-3.9 \
	-i \
	-r ${PROJECT_DIR}/src ${PROJECT_DIR}/include
