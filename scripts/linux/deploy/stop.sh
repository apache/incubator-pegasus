#!/bin/bash

PREFIX=$(readlink -m $(dirname ${BASH_SOURCE}))

cat ${PREFIX}/pid | xargs kill -9 
