#!/bin/bash
# The MIT License (MIT)
#
# Copyright (c) 2015 Microsoft Corporation
#
# -=- Robust Distributed System Nucleus (rDSN) -=-
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


if [ ! -f dsn.replication.simple_kv ]; then
    echo "dsn.replication.simple_kv not exist"
    exit 1
fi

./clear.sh

echo "running dsn.replication.simple_kv for 20 seconds ..."
./dsn.replication.simple_kv config.ini &>out &
PID=$!
sleep 20
kill $PID

if [ -f core ] || ! grep ERR_OK out > /dev/null ; then
    echo "run dsn.replication.simple_kv failed"
    echo "---- ls ----"
    ls -l
    echo "---- head -n 100 out ----"
    head -n 100 out
    if [ -f data/logs/log.1.txt ]; then
        echo "---- tail -n 100 log.1.txt ----"
        tail -n 100 data/logs/log.1.txt
    fi
    if [ -f core ]; then
        echo "---- gdb ./dsn.replication.simple_kv core ----"
        gdb ./dsn.replication.simple_kv core -ex "thread apply all bt" -ex "set pagination 0" -batch
    fi
    exit 1
fi

echo "run dsn.replication.simple_kv succeed"

