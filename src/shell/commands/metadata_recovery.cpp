#include "shell/commands.h"

bool recover(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"node_list_file", required_argument, 0, 'f'},
                                           {"node_list_str", required_argument, 0, 's'},
                                           {"wait_seconds", required_argument, 0, 'w'},
                                           {"skip_bad_nodes", no_argument, 0, 'b'},
                                           {"skip_lost_partitions", no_argument, 0, 'l'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string node_list_file;
    std::string node_list_str;
    int wait_seconds = 100;
    std::string output_file;
    bool skip_bad_nodes = false;
    bool skip_lost_partitions = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "f:s:w:o:bl", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'f':
            node_list_file = optarg;
            break;
        case 's':
            node_list_str = optarg;
            break;
        case 'w':
            if (!dsn::buf2int32(optarg, wait_seconds)) {
                fprintf(stderr, "parse %s as wait_seconds failed\n", optarg);
                return false;
            }
            break;
        case 'o':
            output_file = optarg;
            break;
        case 'b':
            skip_bad_nodes = true;
            break;
        case 'l':
            skip_lost_partitions = true;
            break;
        default:
            return false;
        }
    }

    if (wait_seconds <= 0) {
        fprintf(stderr, "invalid wait_seconds %d, should be positive number\n", wait_seconds);
        return false;
    }

    if (node_list_str.empty() && node_list_file.empty()) {
        fprintf(stderr, "should specify one of node_list_file/node_list_str\n");
        return false;
    }

    if (!node_list_str.empty() && !node_list_file.empty()) {
        fprintf(stderr, "should only specify one of node_list_file/node_list_str\n");
        return false;
    }

    std::vector<dsn::rpc_address> node_list;
    if (!node_list_str.empty()) {
        std::vector<std::string> tokens;
        dsn::utils::split_args(node_list_str.c_str(), tokens, ',');
        if (tokens.empty()) {
            fprintf(stderr, "can't parse node from node_list_str\n");
            return true;
        }

        for (std::string &token : tokens) {
            dsn::rpc_address node;
            if (!node.from_string_ipv4(token.c_str())) {
                fprintf(stderr, "parse %s as a ip:port node failed\n", token.c_str());
                return true;
            }
            node_list.push_back(node);
        }
    } else {
        std::ifstream file(node_list_file);
        if (!file) {
            fprintf(stderr, "open file %s failed\n", node_list_file.c_str());
            return true;
        }

        std::string str;
        int lineno = 0;
        while (std::getline(file, str)) {
            lineno++;
            boost::trim(str);
            if (str.empty() || str[0] == '#' || str[0] == ';')
                continue;
            dsn::rpc_address node;
            if (!node.from_string_ipv4(str.c_str())) {
                fprintf(stderr,
                        "parse %s at file %s line %d as ip:port failed\n",
                        str.c_str(),
                        node_list_file.c_str(),
                        lineno);
                return true;
            }
            node_list.push_back(node);
        }

        if (node_list.empty()) {
            fprintf(stderr, "no node specified in file %s\n", node_list_file.c_str());
            return true;
        }
    }

    dsn::error_code ec = sc->ddl_client->do_recovery(
        node_list, wait_seconds, skip_bad_nodes, skip_lost_partitions, output_file);
    if (!output_file.empty()) {
        std::cout << "recover complete with err = " << ec.to_string() << std::endl;
    }
    return true;
}
