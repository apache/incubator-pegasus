import csv


# ./collector_table_counter_gen.py {counters_file}
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("please input right counter config file path: ./collector_table_counter_gen.py {counters_file}")
    counters_config_file_path = sys.argv[1]

    counter_list = parse_replica_counter(counters_config_file_path)
    for counter in counter_list:
        generate_code_in_info_collector_cpp(counter)
        generate_code_in_info_collector_header(counter)
        generate_code_in_command_helper_header(counter)

    format_code()


def parse_replica_counter(counters_config_file_path):
    counter_list = []

    if not os.path.exists(counters_config_file_path):
        fatalError("no such template file: %s" % counters_config_file_path)

    with open(counters_config_file_path, 'r') as f:
           reader = csv.reader(f)
           for  item in reader:
               if reader.line_num == 1:
                   continue
                counter_list.append(item[0])
            f.close()
            counter_list
