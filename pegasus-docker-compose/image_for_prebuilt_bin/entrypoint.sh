#!/bin/bash

if [ -f /pegasus/bin/config_hdfs.sh ]; then
    source /pegasus/bin/config_hdfs.sh
fi

/pegasus/bin/pegasus_server /pegasus/bin/config.ini -app_list "$1"
