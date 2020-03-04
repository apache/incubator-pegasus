#!/usr/bin/env bash

exit_if_fail() {
    if [ $1 != 0 ]; then
        echo $2
        exit 1
    fi
}

./pegasus_unit_test

exit_if_fail $? "run unit test failed"

