// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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
        callback = [&os, sc](
            int64_t decree, int64_t timestamp, dsn::message_ex **requests, int count) mutable {
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
