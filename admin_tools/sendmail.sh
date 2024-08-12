#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

### send alert email
### usage:
###    bash sendmail.sh alert <alert_email_address> <cluster_name> <table_name> <partition_index>
function send_alert_email()
{
    email_address=$2
    cluster_name=$3
    table_name=$4
    partition_index=$5

    echo "this is a dummy sendmail command, email_to:$2,cluster:$3,table:$4,partition:$5"
}

### send availability_info_email
### usage:
###     bash sendmail.sh availability_info <email_address> <cluster_name> <total_count> <fail_count> <date>
function send_availability_info_email()
{
    email_address=$2
    cluster_name=$3
    total_count=$4
    fail_count=$5
    date=$6

    succ_count=$((total_count-fail_count))

    stat_file=./.availability_info
    total_available="`echo "$succ_count $total_count" | awk '{print $1/$2*100}'`%"
    echo "Statistics from collector on `hostname`" >$stat_file
    echo >>$stat_file
    echo "Detect total count: $total_count" >>$stat_file
    echo "Detect succeed count: $succ_count" >>$stat_file
    echo "Detect failed count: $fail_count" >>$stat_file
    echo "Available: $total_available" >>$stat_file

    echo "this is a dummy sendmail command, email_to:$2,cluster:$3,date:$6,total_count:$4,fail_count:$5,availability:$total_available"
}

###############

if [ $# -eq 0 ]; then
    usage
    exit 0
fi
cmd=$1
case $cmd in
    alert)
        send_alert_email $*
        ;;
    availability_info)
        send_availability_info_email $*
        ;;
    *)
        echo "Error: Unknown command $cmd."
        exit 1
esac
