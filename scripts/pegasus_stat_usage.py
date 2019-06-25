#!/usr/bin/env python                                                                                                                                                                       
# -*- coding: utf-8 -*-
 
#
# This script will calculate sum of storage_size/read_cu/write_cu by app_id and date.
#
# input: <stat_result_file> [stat_result_file]...
#
# output: app_id,date,storage_size_sum,read_cu_sum,write_cu_sum
#

import requests
import json
import re
import sys
import os
import copy

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "USAGE: python %s <stat_result_file> [stat_result_file]..." % sys.argv[0]
        sys.exit(1)

    # stat = { app_id : { date : [ storage_size_sum , read_cu_sum, write_cu_sum ] } }
    stat = {}
    files = sys.argv[1:]
    for file in files:
        with open(file) as fp:
            for cnt, line in enumerate(fp):
                #print("line {} contents {}".format(cnt, line))
                line = line[1:-2]
                fields = line.split("\" : \"")
                hashKey = fields[0]
                date = hashKey.split()[0]
                fields = fields[1].split("\" => \"")
                sortKey = fields[0]
                value = fields[1].replace("\\\"", "\"")
                #print("{} # {} # {}".format(hashKey, sortKey, value))
                if sortKey.startswith("cu@"):
                    decode = json.loads(value)
                    for cu_item in decode.items():
                        app_id = int(cu_item[0])
                        read_cu = cu_item[1][0]
                        write_cu = cu_item[1][1]
                        #print("cu[{}] = [{}, {}]".format(app_id, read_cu, write_cu))
                        if app_id not in stat:
                            stat[app_id] = {}
                        stat_value = stat[app_id]
                        if date not in stat_value:
                            stat_value[date] = [0, 0, 0]
                        date_value = stat_value[date]
                        date_value[1] += read_cu
                        date_value[2] += write_cu
                elif sortKey == "ss":
                    decode = json.loads(value)
                    for ss_item in decode.items():
                        app_id = int(ss_item[0])
                        app_partition_count = ss_item[1][0]
                        stat_partition_count = ss_item[1][1]
                        storage_size = ss_item[1][2]
                        #print("ss[{}] = [{}, {}, {}]".format(app_id, app_partition_count, stat_partition_count, storage_size))
                        if app_id not in stat:
                            stat[app_id] = {}
                        stat_value = stat[app_id]
                        if date not in stat_value:
                            stat_value[date] = [0, 0, 0]
                        date_value = stat_value[date]
                        date_value[0] += storage_size

    for app_id in sorted(stat):
        stat_value = stat[app_id]
        for date in sorted(stat_value):
            date_value = stat_value[date]
            storage_size = date_value[0]
            read_cu = date_value[1]
            write_cu = date_value[2]
            print("{},{},{},{},{}".format(app_id, date, storage_size, read_cu, write_cu))

