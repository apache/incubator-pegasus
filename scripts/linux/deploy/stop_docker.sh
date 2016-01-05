#!/bin/bash

PREFIX=$(readlink -m $(dirname ${BASH_SOURCE}))

cat ${PREFIX}/containerid | xargs docker stop | xargs docker rm
