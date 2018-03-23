#!/bin/bash

LOCAL_IP=`hostname -I | awk '{print $1}'`
sed "s/@LOCAL_IP@/$LOCAL_IP/g" config-sample.ini > config.ini
LD_LIBRARY_PATH=`pwd`/../../DSN_ROOT/lib ./pegasus_cpp_sample onebox temp
