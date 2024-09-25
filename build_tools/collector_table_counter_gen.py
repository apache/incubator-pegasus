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

# This script tool is for generate table-level rocksdb perf counters based on replica-level perf counter.
# Since many rocksdb perf counters need to be aggregated into total value, the code is usually repeated,
# for example, `put_qps` and `get_qps`, this tool generates these codes. you need:
# ```
#python3 ./collector_table_counter_gen.py put_qps, get_qps
# ```
#
# Notice:  The tool is only for generating `total-aggregate` type code. If you want to get `average-aggregate`
# or other type code, please modify the generated code

import os
import sys

root = os.path.split(os.path.realpath(__file__))[0].split("scripts")[0]

info_collector_header_path = root + "/src/server/info_collector.h"
info_collector_cpp_path = root + "./src/server/info_collector.cpp"
command_helper_header_path = root + "./src/shell/command_helper.h"


class Appender(object):
    def __init__(self, seek_line, new_line):
        self.seek_line = seek_line
        self.new_line = new_line
        self.seek_match = False

    def __str__(self):
        return "%s=>%s in %d\n" % (
            self.seek_line, self.new_line, self.seek_pos)


def fatal_error(error):
    print("ERROR: " + error)
    sys.exit(1)


def append_line(filePath, appends):
    if not os.path.exists(filePath):
        fatal_error("no such file: %s" % filePath)

    lines = []
    with open(filePath, 'r+') as fp:
        for line in fp.readlines():
            lines.append(line)
            for append in appends:
                if append.new_line in line:
                    fatal_error(
                        "has added the counter for table, new_line = %s" %
                        append.new_line)
                if append.seek_match:
                    continue
                if append.seek_line in line:
                    lines.append(append.new_line)
                    append.seek_match = True

    for append in appends:
        if not append.seek_match:
            fatal_error(
                "can't match the seek_line(%s), please check the file" %
                append.seek_line)
    with open(filePath, 'w+') as fp:
        fp.write("".join(lines))


def generate_code_in_info_collector_header(replica_counter):
    table_counter = replica_counter.replace(".", "_")
    appends = [
        Appender(
            "write_bytes->set(row_stats.get_total_write_bytes());",
            "%s->set(row_stats.%s);" %
            (table_counter, table_counter)),
        Appender(
            "::dsn::perf_counter_wrapper write_bytes;",
            "::dsn::perf_counter_wrapper %s;" %
            table_counter)]
    append_line(info_collector_header_path, appends)


def generate_code_in_info_collector_cpp(replica_counter):
    table_counter = replica_counter.replace(".", "_")
    appends = [Appender(
        "INIT_COUNTER(write_bytes);",
        "INIT_COUNTER(%s);" %
        table_counter)]
    append_line(info_collector_cpp_path, appends)


def generate_code_in_command_helper_header(replica_counter):
    table_counter = replica_counter.replace(".", "_")
    appends = [Appender(
        "check_and_mutate_bytes += row.check_and_mutate_bytes;",
        "%s += row.%s;" % (table_counter, table_counter)), Appender(
        "double check_and_mutate_bytes = 0;",
        "double %s = 0;" %
        table_counter),
        Appender(
            "row.check_and_mutate_bytes += value;",
            " else if (counter_name == \"%s\")  row.%s += value;" %
            (replica_counter, table_counter))]
    append_line(command_helper_header_path, appends)


# python3 ./collector_table_counter_gen.py counter1,counter2
# please use `./build_tools/format_files.sh` to format after generate code
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("python3 ./collector_table_counter_gen.py {counter1,counter2..}")

    counter_list = sys.argv[1].split(",")
    for counter in counter_list:
        generate_code_in_info_collector_header(counter)
        generate_code_in_info_collector_cpp(counter)
        generate_code_in_command_helper_header(counter)
