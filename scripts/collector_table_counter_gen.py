import os
import sys

info_collector_header_path = "./src/server/info_collector.h"
info_collector_cpp_path = "./src/server/info_collector.cpp"
command_helper_header_path = "./src/shell/command_helper.h"


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

    fp = open(filePath, 'r+')
    lines = []
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

    fp.close()
    for append in appends:
        if not append.seek_match:
            fatal_error(
                "can't match the seek line(%s), please check the file if update" %
                append.seek_line)
    fp = open(filePath, 'w+')
    fp.write("".join(lines))
    fp.close()


def generate_code_in_info_collector_header(counter):
    appends = [
        Appender(
            "write_bytes->set(row_stats.get_total_write_bytes());",
            "%s->set(row_stats.%s);" %
            (counter, counter)),
        Appender(
            "::dsn::perf_counter_wrapper write_bytes;",
            "::dsn::perf_counter_wrapper %s;" %
            counter)]
    append_line(info_collector_header_path, appends)


def generate_code_in_info_collector_cpp(counter):
    appends = [Appender(
        "INIT_COUNTER(write_bytes);",
        "INIT_COUNTER(%s);" %
        counter)]
    append_line(info_collector_cpp_path, appends)


def generate_code_in_command_helper_header(counter):
    appends = [Appender(
        "check_and_mutate_bytes += row.check_and_mutate_bytes;",
        "%s += row.%s;" % (counter, counter)), Appender(
        "double check_and_mutate_bytes = 0;",
        "double %s = 0;" %
        counter),
        Appender(
            "row.check_and_mutate_bytes += value;",
            " else if (counter_name == \"%s\")  row.%s += value;" %
            (counter, counter))]
    append_line(command_helper_header_path, appends)


# ./collector_table_counter_gen.py {counters_file}
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("./collector_table_counter_gen.py {counter1,counter2..}")

    counter_list = sys.argv[1].split(",")
    for counter in counter_list:
        generate_code_in_info_collector_header(counter)
        generate_code_in_info_collector_cpp(counter)
        generate_code_in_command_helper_header(counter)
