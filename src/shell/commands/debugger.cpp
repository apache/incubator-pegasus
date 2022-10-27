/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "shell/commands.h"
#include "base/idl_utils.h"
#include <rocksdb/sst_dump_tool.h>
#include <rocksdb/utilities/ldb_cmd.h>
#include <fmt/time.h>

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
                CHECK_NOTNULL(request, "");
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
                } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
                    dsn::apps::check_and_set_request update;
                    dsn::unmarshall(request, update);
                    auto set_sort_key =
                        update.set_diff_sort_key ? update.set_sort_key : update.check_sort_key;
                    std::string check_operand;
                    if (pegasus::cas_is_check_operand_needed(update.check_type)) {
                        check_operand = fmt::format(
                            "\"{}\" ",
                            pegasus::utils::c_escape_string(update.check_operand, sc->escape_all));
                    }
                    os << INDENT
                       << fmt::format(
                              "[CHECK_AND_SET] \"{}\" : IF SORT_KEY({}) {} {}"
                              "THEN SET SORT_KEY({}) => VALUE({}) [expire={}]\n",
                              pegasus::utils::c_escape_string(update.hash_key, sc->escape_all),
                              pegasus::utils::c_escape_string(update.check_sort_key,
                                                              sc->escape_all),
                              pegasus::cas_check_type_to_string(update.check_type),
                              check_operand,
                              pegasus::utils::c_escape_string(set_sort_key, sc->escape_all),
                              pegasus::utils::c_escape_string(update.set_value, sc->escape_all),
                              update.set_expire_ts_seconds);
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

bool rdb_key_str2hex(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }
    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    ::dsn::blob key;
    pegasus::pegasus_generate_key(key, hash_key, sort_key);
    rocksdb::Slice skey(key.data(), key.length());
    fprintf(stderr, "\"%s\"\n", skey.ToString(true).c_str());
    return true;
}

bool rdb_key_hex2str(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 2) {
        return false;
    }
    std::string hex_rdb_key = sds_to_string(args.argv[1]);
    dsn::blob key = dsn::blob::create_from_bytes(rocksdb::LDBCommand::HexToString(hex_rdb_key));
    std::string hash_key, sort_key;
    pegasus::pegasus_restore_key(key, hash_key, sort_key);
    fmt::print(
        stderr, "\nhash key: \"{}\"\n", pegasus::utils::c_escape_string(hash_key, sc->escape_all));
    fmt::print(
        stderr, "\nsort key: \"{}\"\n", pegasus::utils::c_escape_string(sort_key, sc->escape_all));
    return true;
}

bool rdb_value_hex2str(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 2) {
        return false;
    }
    std::string hex_rdb_value = sds_to_string(args.argv[1]);
    std::string pegasus_value = rocksdb::LDBCommand::HexToString(hex_rdb_value);
    auto expire_ts = static_cast<int64_t>(pegasus::pegasus_extract_expire_ts(0, pegasus_value)) +
                     pegasus::utils::epoch_begin; // TODO(wutao): pass user specified version
    std::time_t tm(expire_ts);
    fmt::print(stderr, "\nWhen to expire:\n  {:%Y-%m-%d %H:%M:%S}\n", *std::localtime(&tm));

    dsn::blob user_data;
    pegasus::pegasus_extract_user_data(0, std::move(pegasus_value), user_data);
    fprintf(stderr,
            "user_data:\n  \"%s\"\n",
            pegasus::utils::c_escape_string(user_data.to_string(), sc->escape_all).c_str());
    return true;
}
