#!/bin/bash

sed "s/@LOCAL_IP@/`hostname -i`/g" config-sample.ini > config.ini
LD_LIBRARY_PATH=`pwd`/../../DSN_ROOT/lib ./pegasus_cpp_sample onebox temp
