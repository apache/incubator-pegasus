import csv

info_collector_header_path = "../src/server/info_collector.h"
info_collector_cpp_path = "../src/server/info_collector.cpp"
command_helper_header = "../src/shell/command_helper.h"

# ./collector_table_counter_gen.py {counters_file}
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(
            "please input right counter config file path: ./collector_table_counter_gen.py {counters_file}")
    counters_config_file_path = sys.argv[1]

    counter_list = parse_replica_counter(counters_config_file_path)
    for counter in counter_list:
        generate_code_in_info_collector_header(counter)
        generate_code_in_info_collector_cpp(counter)
        generate_code_in_command_helper_header(counter)

    format_code()


def parse_replica_counter(counters_config_file_path):
    counter_list = []

    if not os.path.exists(counters_config_file_path):
        fatalError("no such template file: %s" % counters_config_file_path)

    with open(counters_config_file_path, 'r') as f:
           reader = csv.reader(f)
           for item in reader:
               if reader.line_num == 1:
                   continue
                counter_list.append(item[0])
            f.close()
            counter_list

class Appender(object):
    def __init__(self, seel_line, new_line):
        seek_line = ''
        new_line = ''

        seek_pos = 0

def generate_code_in_info_collector_header(counter):
    appends = []
    appends.append(Appender("get_qps->set(row_stats.get_qps);",  "%s->set(row_stats.%s);" % counter, counter))
    appends.append(Appender("double get_qps = 0;",  "double %s = 0;" % counter))
    appends.append(Appender("if (counter_name == \"get_qps\")",  " else if (counter_name == \"%s\") \n row.%s += value;" % counter))
    append_line(info_collector_cpp)

def generate_code_in_info_collector_cpp(counter):
    appends = []
    appends.append(Appender("INIT_COUNTER(get_qps)",  "INIT_COUNTER(%s);" % counter))
    append_line(info_collector_header, appenders)

def generate_code_in_command_helper_header(counter):
    appends = []
    appends.append(Appender("get_qps += row.get_qps;",  "%s += row.%s;" % counter, counter))
    appends.append(Appender("::dsn::perf_counter_wrapper get_qps;",  "dsn::perf_counter_wrapper %s;" % counter))

def append_line(filePath, appends) {
    if not os.path.exists(file):
        fatalError("no such file: %s" % file)

    fp = file(filePath)
    lines = []
    pos = 0
    for line in fp:
        lines.append(line)
        pos += 1
        for append in appends:
            if append.seek_pos != 0:
                continue
            if line == append.seek_line:
                append.seek_pos = pos
                break
    fp.close()
    for append in appends:
        lines.insert(append.seek_pos - 1, append.new_line) # insert previous line
    s = '\n'.join(lines)
    fp = file(file 'w')
    fp.write(s)
    fp.close()
}
