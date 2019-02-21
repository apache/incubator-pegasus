#include "shell/commands.h"

bool sst_dump(command_executor *e, shell_context *sc, arguments args)
{
    rocksdb::SSTDumpTool tool;
    tool.Run(args.argc, args.argv);
    return true;
}

bool mlog_dump(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"input", required_argument, 0, 'i'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    bool detailed = false;
    std::string input;
    std::string output;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "di:o:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'i':
            input = optarg;
            break;
        case 'o':
            output = optarg;
            break;
        default:
            return false;
        }
    }
    if (input.empty()) {
        fprintf(stderr, "ERROR: input is not specified\n");
        return false;
    }
    if (!dsn::utils::filesystem::directory_exists(input)) {
        fprintf(stderr, "ERROR: input %s is not a directory\n", input.c_str());
        return false;
    }

    std::ostream *os_ptr = nullptr;
    if (output.empty()) {
        os_ptr = &std::cout;
    } else {
        os_ptr = new std::ofstream(output);
        if (!*os_ptr) {
            fprintf(stderr, "ERROR: open output file %s failed\n", output.c_str());
            delete os_ptr;
            return true;
        }
    }
    std::ostream &os = *os_ptr;

    std::function<void(int64_t decree, int64_t timestamp, dsn::message_ex * *requests, int count)>
        callback;
    if (detailed) {
        callback = [&os, sc](int64_t decree,
                             int64_t timestamp,
                             dsn::message_ex **requests,
                             int count) mutable {
            for (int i = 0; i < count; ++i) {
                dsn::message_ex *request = requests[i];
                dassert(request != nullptr, "");
                ::dsn::message_ex *msg = (::dsn::message_ex *)request;
                if (msg->local_rpc_code == RPC_REPLICATION_WRITE_EMPTY) {
                    os << INDENT << "[EMPTY]" << std::endl;
                } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_PUT) {
                    ::dsn::apps::update_request update;
                    ::dsn::unmarshall(request, update);
                    std::string hash_key, sort_key;
                    pegasus::pegasus_restore_key(update.key, hash_key, sort_key);
                    os << INDENT << "[PUT] \""
                       << pegasus::utils::c_escape_string(hash_key, sc->escape_all) << "\" : \""
                       << pegasus::utils::c_escape_string(sort_key, sc->escape_all) << "\" => "
                       << update.expire_ts_seconds << " : \""
                       << pegasus::utils::c_escape_string(update.value, sc->escape_all) << "\""
                       << std::endl;
                } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                    ::dsn::blob key;
                    ::dsn::unmarshall(request, key);
                    std::string hash_key, sort_key;
                    pegasus::pegasus_restore_key(key, hash_key, sort_key);
                    os << INDENT << "[REMOVE] \""
                       << pegasus::utils::c_escape_string(hash_key, sc->escape_all) << "\" : \""
                       << pegasus::utils::c_escape_string(sort_key, sc->escape_all) << "\""
                       << std::endl;
                } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
                    ::dsn::apps::multi_put_request update;
                    ::dsn::unmarshall(request, update);
                    os << INDENT << "[MULTI_PUT] " << update.kvs.size() << std::endl;
                    for (::dsn::apps::key_value &kv : update.kvs) {
                        os << INDENT << INDENT << "[PUT] \""
                           << pegasus::utils::c_escape_string(update.hash_key, sc->escape_all)
                           << "\" : \"" << pegasus::utils::c_escape_string(kv.key, sc->escape_all)
                           << "\" => " << update.expire_ts_seconds << " : \""
                           << pegasus::utils::c_escape_string(kv.value, sc->escape_all) << "\""
                           << std::endl;
                    }
                } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
                    ::dsn::apps::multi_remove_request update;
                    ::dsn::unmarshall(request, update);
                    os << INDENT << "[MULTI_REMOVE] " << update.sort_keys.size() << std::endl;
                    for (::dsn::blob &sort_key : update.sort_keys) {
                        os << INDENT << INDENT << "[REMOVE] \""
                           << pegasus::utils::c_escape_string(update.hash_key, sc->escape_all)
                           << "\" : \"" << pegasus::utils::c_escape_string(sort_key, sc->escape_all)
                           << "\"" << std::endl;
                    }
                } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_INCR) {
                    ::dsn::apps::incr_request update;
                    ::dsn::unmarshall(request, update);
                    std::string hash_key, sort_key;
                    pegasus::pegasus_restore_key(update.key, hash_key, sort_key);
                    os << INDENT << "[INCR] \""
                       << pegasus::utils::c_escape_string(hash_key, sc->escape_all) << "\" : \""
                       << pegasus::utils::c_escape_string(sort_key, sc->escape_all) << "\" => "
                       << update.increment << std::endl;
                } else {
                    os << INDENT << "ERROR: unsupported code "
                       << ::dsn::task_code(msg->local_rpc_code).to_string() << "("
                       << msg->local_rpc_code << ")" << std::endl;
                }
            }
        };
    }

    dsn::replication::mutation_log_tool tool;
    bool ret = tool.dump(input, os, callback);
    if (!ret) {
        fprintf(stderr, "ERROR: dump failed\n");
    } else {
        fprintf(stderr, "Done\n");
    }

    if (os_ptr != &std::cout) {
        delete os_ptr;
    }

    return true;
}

bool local_get(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 4) {
        return false;
    }

    std::string db_path = args.argv[1];
    std::string hash_key = args.argv[2];
    std::string sort_key = args.argv[3];

    rocksdb::Options db_opts;
    rocksdb::DB *db;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(db_opts, db_path, &db);
    if (!status.ok()) {
        fprintf(stderr, "ERROR: open db failed: %s\n", status.ToString().c_str());
        return true;
    }

    ::dsn::blob key;
    pegasus::pegasus_generate_key(key, hash_key, sort_key);
    rocksdb::Slice skey(key.data(), key.length());
    std::string value;
    rocksdb::ReadOptions rd_opts;
    status = db->Get(rd_opts, skey, &value);
    if (!status.ok()) {
        fprintf(stderr, "ERROR: get failed: %s\n", status.ToString().c_str());
    } else {
        uint32_t expire_ts = pegasus::pegasus_extract_expire_ts(0, value);
        dsn::blob user_data;
        pegasus::pegasus_extract_user_data(0, std::move(value), user_data);
        fprintf(stderr,
                "%u : \"%s\"\n",
                expire_ts,
                pegasus::utils::c_escape_string(user_data, sc->escape_all).c_str());
    }

    delete db;
    return true;
}

dsn::rpc_address diagnose_recommend(const ddd_partition_info &pinfo)
{
    if (pinfo.config.last_drops.size() < 2)
        return dsn::rpc_address();

    std::vector<dsn::rpc_address> last_two_nodes(pinfo.config.last_drops.end() - 2,
                                                 pinfo.config.last_drops.end());
    std::vector<ddd_node_info> last_dropped;
    for (auto &node : last_two_nodes) {
        auto it = std::find_if(pinfo.dropped.begin(),
                               pinfo.dropped.end(),
                               [&node](const ddd_node_info &r) { return r.node == node; });
        if (it->is_alive && it->is_collected)
            last_dropped.push_back(*it);
    }

    if (last_dropped.size() == 1) {
        const ddd_node_info &ninfo = last_dropped.back();
        if (ninfo.last_committed_decree >= pinfo.config.last_committed_decree)
            return ninfo.node;
    } else if (last_dropped.size() == 2) {
        const ddd_node_info &secondary = last_dropped.front();
        const ddd_node_info &latest = last_dropped.back();

        // Select a best node to be the new primary, following the rule:
        //  - choose the node with the largest last committed decree
        //  - if last committed decree is the same, choose node with the largest ballot

        if (latest.last_committed_decree == secondary.last_committed_decree &&
            latest.last_committed_decree >= pinfo.config.last_committed_decree)
            return latest.ballot >= secondary.ballot ? latest.node : secondary.node;

        if (latest.last_committed_decree > secondary.last_committed_decree &&
            latest.last_committed_decree >= pinfo.config.last_committed_decree)
            return latest.node;

        if (secondary.last_committed_decree > latest.last_committed_decree &&
            secondary.last_committed_decree >= pinfo.config.last_committed_decree)
            return secondary.node;
    }

    return dsn::rpc_address();
}

bool ddd_diagnose(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"gpid", required_argument, 0, 'g'},
                                           {"diagnose", no_argument, 0, 'd'},
                                           {"auto_diagnose", no_argument, 0, 'a'},
                                           {"skip_prompt", no_argument, 0, 's'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string out_file;
    dsn::gpid id(-1, -1);
    bool diagnose = false;
    bool auto_diagnose = false;
    bool skip_prompt = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "g:daso:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'g':
            int pid;
            if (id.parse_from(optarg)) {
                // app_id.partition_index
            } else if (sscanf(optarg, "%d", &pid) == 1) {
                // app_id
                id.set_app_id(pid);
            } else {
                fprintf(stderr, "ERROR: invalid gpid %s\n", optarg);
                return false;
            }
            break;
        case 'd':
            diagnose = true;
            break;
        case 'a':
            auto_diagnose = true;
            break;
        case 's':
            skip_prompt = true;
            break;
        case 'o':
            out_file = optarg;
            break;
        default:
            return false;
        }
    }

    std::vector<ddd_partition_info> ddd_partitions;
    ::dsn::error_code ret = sc->ddl_client->ddd_diagnose(id, ddd_partitions);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "ERROR: DDD diagnose failed with err = %s\n", ret.to_string());
        return true;
    }

    std::streambuf *buf;
    std::ofstream of;

    if (!out_file.empty()) {
        of.open(out_file);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    out << "Total " << ddd_partitions.size() << " ddd partitions:" << std::endl;
    out << std::endl;
    int proposed_count = 0;
    int i = 0;
    for (const ddd_partition_info &pinfo : ddd_partitions) {
        out << "(" << ++i << ") " << pinfo.config.pid.to_string() << std::endl;
        out << "    config: ballot(" << pinfo.config.ballot << "), "
            << "last_committed(" << pinfo.config.last_committed_decree << ")" << std::endl;
        out << "    ----" << std::endl;
        dsn::rpc_address latest_dropped, secondary_latest_dropped;
        if (pinfo.config.last_drops.size() > 0)
            latest_dropped = pinfo.config.last_drops[pinfo.config.last_drops.size() - 1];
        if (pinfo.config.last_drops.size() > 1)
            secondary_latest_dropped = pinfo.config.last_drops[pinfo.config.last_drops.size() - 2];
        int j = 0;
        for (const ddd_node_info &n : pinfo.dropped) {
            char time_buf[30];
            ::dsn::utils::time_ms_to_string(n.drop_time_ms, time_buf);
            out << "    dropped[" << j++ << "]: "
                << "node(" << n.node.to_string() << "), "
                << "drop_time(" << time_buf << "), "
                << "alive(" << (n.is_alive ? "true" : "false") << "), "
                << "collected(" << (n.is_collected ? "true" : "false") << "), "
                << "ballot(" << n.ballot << "), "
                << "last_committed(" << n.last_committed_decree << "), "
                << "last_prepared(" << n.last_prepared_decree << ")";
            if (n.node == latest_dropped)
                out << "  <== the latest";
            else if (n.node == secondary_latest_dropped)
                out << "  <== the secondary latest";
            out << std::endl;
        }
        out << "    ----" << std::endl;
        j = 0;
        for (const ::dsn::rpc_address &r : pinfo.config.last_drops) {
            out << "    last_drops[" << j++ << "]: "
                << "node(" << r.to_string() << ")";
            if (j == (int)pinfo.config.last_drops.size() - 1)
                out << "  <== the secondary latest";
            else if (j == (int)pinfo.config.last_drops.size())
                out << "  <== the latest";
            out << std::endl;
        }
        out << "    ----" << std::endl;
        out << "    ddd_reason: " << pinfo.reason << std::endl;
        if (diagnose) {
            out << "    ----" << std::endl;

            dsn::rpc_address primary = diagnose_recommend(pinfo);
            out << "    recommend_primary: "
                << (primary.is_invalid() ? "none" : primary.to_string());
            if (primary == latest_dropped)
                out << "  <== the latest";
            else if (primary == secondary_latest_dropped)
                out << "  <== the secondary latest";
            out << std::endl;

            bool skip_this = false;
            if (!primary.is_invalid() && !auto_diagnose && !skip_prompt) {
                do {
                    std::cout << "    > Are you sure to use the recommend primary? [y/n/s(skip)]: ";
                    char c;
                    std::cin >> c;
                    if (c == 'y') {
                        break;
                    } else if (c == 'n') {
                        primary.set_invalid();
                        break;
                    } else if (c == 's') {
                        skip_this = true;
                        std::cout << "    > You have choosed to skip diagnosing this partition."
                                  << std::endl;
                        break;
                    }
                } while (true);
            }

            if (primary.is_invalid() && !skip_prompt && !skip_this) {
                do {
                    std::cout << "    > Please input the primary node: ";
                    std::string addr;
                    std::cin >> addr;
                    if (primary.from_string_ipv4(addr.c_str())) {
                        break;
                    } else {
                        std::cout << "    > Sorry, you have input an invalid node address."
                                  << std::endl;
                    }
                } while (true);
            }

            if (!primary.is_invalid() && !skip_this) {
                dsn::replication::configuration_balancer_request request;
                request.gpid = pinfo.config.pid;
                request.action_list = {configuration_proposal_action{
                    primary, primary, config_type::CT_ASSIGN_PRIMARY}};
                request.force = false;
                dsn::error_code err = sc->ddl_client->send_balancer_proposal(request);
                out << "    propose_request: propose -g " << request.gpid.to_string()
                    << " -p ASSIGN_PRIMARY -t " << primary.to_string() << " -n "
                    << primary.to_string() << std::endl;
                out << "    propose_response: " << err.to_string() << std::endl;
                proposed_count++;
            } else {
                out << "    propose_request: none" << std::endl;
            }
        }
        out << std::endl;
        out << "Proposed count: " << proposed_count << "/" << ddd_partitions.size() << std::endl;
        out << std::endl;
    }

    std::cout << "Diagnose ddd done." << std::endl;
    return true;
}