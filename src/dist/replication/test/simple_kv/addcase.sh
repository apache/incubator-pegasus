#!/bin/bash

if [ $# -ne 2 ]; then
    echo "USAGE: $0 <new-case-id> <from-case-id>"
    echo " e.g.: $0 106 100"
    exit -1
fi

id=$1

if [ -f case-${id}.act ]; then
    echo "case ${id} already exists"
    exit -1
fi

old=$2
cp case-${old}.act case-${id}.act
cp case-${old}.ini case-${id}.ini

