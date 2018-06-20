// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <getopt.h>
#include <thread>
#include <iomanip>
#include <fstream>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <rocksdb/db.h>
#include <rocksdb/sst_dump_tool.h>
#include <dsn/utility/filesystem.h>
#include <dsn/tool/cli/cli.client.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/dist/replication/mutation_log_tool.h>

#include <rrdb/rrdb.code.definition.h>
#include <rrdb/rrdb.types.h>
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <pegasus/error.h>

#include "command_executor.h"
#include "command_utils.h"
#include "command_helper.h"
#include "args.h"

using namespace dsn::replication;

inline bool version(command_executor *e, shell_context *sc, arguments args)
{
    std::ostringstream oss;
    oss << "Pegasus Shell " << PEGASUS_VERSION << " (" << PEGASUS_GIT_COMMIT << ") "
        << PEGASUS_BUILD_TYPE;
    std::cout << oss.str() << std::endl;
    return true;
}

inline bool
buf2filter_type(const char *buffer, int length, pegasus::pegasus_client::filter_type &result)
{
    if (length == 8 && strncmp(buffer, "anywhere", 8) == 0) {
        result = pegasus::pegasus_client::FT_MATCH_ANYWHERE;
        return true;
    }
    if (length == 6 && strncmp(buffer, "prefix", 6) == 0) {
        result = pegasus::pegasus_client::FT_MATCH_PREFIX;
        return true;
    }
    if (length == 7 && strncmp(buffer, "postfix", 7) == 0) {
        result = pegasus::pegasus_client::FT_MATCH_POSTFIX;
        return true;
    }
    return false;
}

inline bool query_cluster_info(command_executor *e, shell_context *sc, arguments args)
{
    ::dsn::error_code err = sc->ddl_client->cluster_info("");
    if (err == ::dsn::ERR_OK)
        std::cout << "get cluster info succeed" << std::endl;
    else
        std::cout << "get cluster info failed, error=" << err.to_string() << std::endl;
    return true;
}

inline bool query_app(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    static struct option long_options[] = {
        {"detailed", no_argument, 0, 'd'}, {"output", required_argument, 0, 'o'}, {0, 0, 0, 0}};

    std::string app_name = args.argv[1];
    std::string out_file;
    bool detailed = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "do:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'o':
            out_file = optarg;
            break;
        default:
            return false;
        }
    }

    if (!(app_name.empty() && out_file.empty())) {
        std::cout << "[Parameters]" << std::endl;
        if (!app_name.empty())
            std::cout << "app_name: " << app_name << std::endl;
        if (!out_file.empty())
            std::cout << "out_file: " << out_file << std::endl;
    }
    if (detailed)
        std::cout << "detailed: true" << std::endl;
    else
        std::cout << "detailed: false" << std::endl;
    std::cout << std::endl << "[Result]" << std::endl;

    if (app_name.empty()) {
        std::cout << "ERROR: null app name" << std::endl;
        return false;
    }
    ::dsn::error_code err = sc->ddl_client->list_app(app_name, detailed, out_file);
    if (err == ::dsn::ERR_OK)
        std::cout << "list app " << app_name << " succeed" << std::endl;
    else
        std::cout << "list app " << app_name << " failed, error=" << err.to_string() << std::endl;
    return true;
}

inline bool ls_apps(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"all", no_argument, 0, 'a'},
                                           {"detailed", no_argument, 0, 'd'},
                                           {"status", required_argument, 0, 's'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    bool show_all = false;
    bool detailed = false;
    std::string status;
    std::string output_file;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "ads:o:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            show_all = true;
            break;
        case 'd':
            detailed = true;
            break;
        case 's':
            status = optarg;
            break;
        case 'o':
            output_file = optarg;
            break;
        default:
            return false;
        }
    }

    ::dsn::app_status::type s = ::dsn::app_status::AS_INVALID;
    if (!status.empty() && status != "all") {
        s = type_from_string(::dsn::_app_status_VALUES_TO_NAMES,
                             std::string("AS_") + status,
                             ::dsn::app_status::AS_INVALID);
        verify_logged(s != ::dsn::app_status::AS_INVALID,
                      "parse %s as app_status::type failed",
                      status.c_str());
    }
    ::dsn::error_code err = sc->ddl_client->list_apps(s, show_all, detailed, output_file);
    if (err == ::dsn::ERR_OK)
        std::cout << "list apps succeed" << std::endl;
    else
        std::cout << "list apps failed, error=" << err.to_string() << std::endl;
    return true;
}

inline bool ls_nodes(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"status", required_argument, 0, 's'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string status;
    std::string output_file;
    bool detailed = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "ds:o:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 's':
            status = optarg;
            break;
        case 'o':
            output_file = optarg;
            break;
        default:
            return false;
        }
    }

    if (!(status.empty() && output_file.empty())) {
        std::cout << "[Parameters]" << std::endl;
        if (!status.empty())
            std::cout << "status: " << status << std::endl;
        if (!output_file.empty())
            std::cout << "out_file: " << output_file << std::endl;
        std::cout << std::endl << "[Result]" << std::endl;
    }

    ::dsn::replication::node_status::type s = ::dsn::replication::node_status::NS_INVALID;
    if (!status.empty() && status != "all") {
        s = type_from_string(dsn::replication::_node_status_VALUES_TO_NAMES,
                             std::string("ns_") + status,
                             ::dsn::replication::node_status::NS_INVALID);
        verify_logged(s != ::dsn::replication::node_status::NS_INVALID,
                      "parse %s as node_status::type failed",
                      status.c_str());
    }

    ::dsn::error_code err = sc->ddl_client->list_nodes(s, detailed, output_file);
    if (err != ::dsn::ERR_OK)
        std::cout << "list nodes failed, error=" << err.to_string() << std::endl;
    return true;
}

inline bool create_app(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"partition_count", required_argument, 0, 'p'},
                                           {"replica_count", required_argument, 0, 'r'},
                                           {"envs", required_argument, 0, 'e'},
                                           {0, 0, 0, 0}};

    if (args.argc < 2)
        return false;

    std::string app_name = args.argv[1];

    int pc = 4, rc = 3;
    std::map<std::string, std::string> envs;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:r:e:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), pc)) {
                fprintf(stderr, "parse %s as partition_count failed\n", optarg);
                return false;
            }
            break;
        case 'r':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), rc)) {
                fprintf(stderr, "parse %s as replica_count failed\n", optarg);
                return false;
            }
            break;
        case 'e':
            if (!::dsn::utils::parse_kv_map(optarg, envs, ',', '=')) {
                fprintf(stderr, "invalid envs: %s\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    ::dsn::error_code err = sc->ddl_client->create_app(app_name, "pegasus", pc, rc, envs, false);
    if (err == ::dsn::ERR_OK)
        std::cout << "create app \"" << pegasus::utils::c_escape_string(app_name) << "\" succeed"
                  << std::endl;
    else
        std::cout << "create app \"" << pegasus::utils::c_escape_string(app_name)
                  << "\" failed, error = " << err.to_string() << std::endl;
    return true;
}

inline bool drop_app(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"reserve_seconds", required_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    if (args.argc < 2)
        return false;

    std::string app_name = args.argv[1];

    int reserve_seconds = 0;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "r:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'r':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), reserve_seconds)) {
                fprintf(stderr, "parse %s as reserve_seconds failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    std::cout << "reserve_seconds = " << reserve_seconds << std::endl;
    ::dsn::error_code err = sc->ddl_client->drop_app(app_name, reserve_seconds);
    if (err == ::dsn::ERR_OK)
        std::cout << "drop app " << app_name << " succeed" << std::endl;
    else
        std::cout << "drop app " << app_name << " failed, error=" << err.to_string() << std::endl;
    return true;
}

inline bool recall_app(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    int id;
    std::string new_name = "";
    if (!::pegasus::utils::buf2int(args.argv[1], strlen(args.argv[1]), id)) {
        fprintf(stderr, "ERROR: parse %s as id failed\n", args.argv[1]);
        return false;
    }
    if (args.argc >= 3) {
        new_name = args.argv[2];
    }

    ::dsn::error_code err = sc->ddl_client->recall_app(id, new_name);
    if (dsn::ERR_OK == err)
        std::cout << "recall app " << id << " succeed" << std::endl;
    else
        std::cout << "recall app " << id << " failed, error=" << err.to_string() << std::endl;
    return true;
}

inline bool set_meta_level(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    dsn::replication::meta_function_level::type l;
    l = type_from_string(_meta_function_level_VALUES_TO_NAMES,
                         std::string("fl_") + args.argv[1],
                         meta_function_level::fl_invalid);
    verify_logged(l != meta_function_level::fl_invalid,
                  "parse %s as meta function level failed\n",
                  args.argv[1]);

    configuration_meta_control_response resp = sc->ddl_client->control_meta_function_level(l);
    if (resp.err == dsn::ERR_OK) {
        std::cout << "control meta level ok, the old level is "
                  << _meta_function_level_VALUES_TO_NAMES.find(resp.old_level)->second << std::endl;
    } else {
        std::cout << "control meta level got error " << resp.err.to_string() << std::endl;
    }
    return true;
}

inline bool get_meta_level(command_executor *e, shell_context *sc, arguments args)
{
    configuration_meta_control_response resp = sc->ddl_client->control_meta_function_level(
        dsn::replication::meta_function_level::fl_invalid);
    if (resp.err == dsn::ERR_OK) {
        std::cout << "current meta level is "
                  << _meta_function_level_VALUES_TO_NAMES.find(resp.old_level)->second << std::endl;
    } else {
        std::cout << "get meta level got error " << resp.err.to_string() << std::endl;
    }
    return true;
}

inline bool propose(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"force", no_argument, 0, 'f'},
                                           {"gpid", required_argument, 0, 'g'},
                                           {"type", required_argument, 0, 'p'},
                                           {"target", required_argument, 0, 't'},
                                           {"node", required_argument, 0, 'n'},
                                           {0, 0, 0, 0}};

    dverify(args.argc >= 9);
    dsn::replication::configuration_balancer_request request;
    request.gpid.set_app_id(-1);
    dsn::rpc_address target, node;
    std::string proposal_type = "CT_";
    request.force = false;
    bool ans;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "fg:p:t:n:", long_options, &option_index);
        if (-1 == c)
            break;
        switch (c) {
        case 'f':
            request.force = true;
            break;
        case 'g':
            ans = request.gpid.parse_from(optarg);
            verify_logged(ans, "parse %s as gpid failed\n", optarg);
            break;
        case 'p':
            proposal_type += optarg;
            break;
        case 't':
            verify_logged(
                target.from_string_ipv4(optarg), "parse %s as target_address failed\n", optarg);
            break;
        case 'n':
            verify_logged(node.from_string_ipv4(optarg), "parse %s as node failed\n", optarg);
            break;
        default:
            return false;
        }
    }

    verify_logged(!target.is_invalid(), "need set target by -t\n");
    verify_logged(!node.is_invalid(), "need set node by -n\n");
    verify_logged(request.gpid.get_app_id() != -1, "need set gpid by -g\n");

    config_type::type tp =
        type_from_string(_config_type_VALUES_TO_NAMES, proposal_type, config_type::CT_INVALID);
    verify_logged(
        tp != config_type::CT_INVALID, "parse %s as config_type failed.\n", proposal_type.c_str());
    request.action_list = {configuration_proposal_action{target, node, tp}};
    dsn::error_code err = sc->ddl_client->send_balancer_proposal(request);
    std::cout << "send proposal response: " << err.to_string() << std::endl;
    return true;
}

inline bool balance(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"gpid", required_argument, 0, 'g'},
                                           {"type", required_argument, 0, 'p'},
                                           {"from", required_argument, 0, 'f'},
                                           {"to", required_argument, 0, 't'},
                                           {0, 0, 0, 0}};

    if (args.argc < 9)
        return false;

    dsn::replication::configuration_balancer_request request;
    request.gpid.set_app_id(-1);
    std::string balance_type;
    dsn::rpc_address from, to;
    bool ans;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "g:p:f:t:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'g':
            ans = request.gpid.parse_from(optarg);
            if (!ans) {
                fprintf(stderr, "parse %s as gpid failed\n", optarg);
                return false;
            }
            break;
        case 'p':
            balance_type = optarg;
            break;
        case 'f':
            if (!from.from_string_ipv4(optarg)) {
                fprintf(stderr, "parse %s as from_address failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!to.from_string_ipv4(optarg)) {
                fprintf(stderr, "parse %s as target_address failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    std::vector<configuration_proposal_action> &actions = request.action_list;
    actions.reserve(4);
    if (balance_type == "move_pri") {
        actions.emplace_back(
            configuration_proposal_action{from, from, config_type::CT_DOWNGRADE_TO_SECONDARY});
        actions.emplace_back(
            configuration_proposal_action{to, to, config_type::CT_UPGRADE_TO_PRIMARY});
    } else if (balance_type == "copy_pri") {
        actions.emplace_back(
            configuration_proposal_action{from, to, config_type::CT_ADD_SECONDARY_FOR_LB});
        actions.emplace_back(
            configuration_proposal_action{from, from, config_type::CT_DOWNGRADE_TO_SECONDARY});
        actions.emplace_back(
            configuration_proposal_action{to, to, config_type::CT_UPGRADE_TO_PRIMARY});
    } else if (balance_type == "copy_sec") {
        actions.emplace_back(configuration_proposal_action{
            dsn::rpc_address(), to, config_type::CT_ADD_SECONDARY_FOR_LB});
        actions.emplace_back(configuration_proposal_action{
            dsn::rpc_address(), from, config_type::CT_DOWNGRADE_TO_INACTIVE});
    } else {
        fprintf(stderr, "parse %s as a balance type failed\n", balance_type.c_str());
        return false;
    }

    if (from.is_invalid()) {
        fprintf(stderr, "need set from address by -f\n");
        return false;
    }
    if (to.is_invalid()) {
        fprintf(stderr, "need set target address by -t\n");
        return false;
    }
    if (request.gpid.get_app_id() == -1) {
        fprintf(stderr, "need set the gpid by -g\n");
        return false;
    }
    dsn::error_code ec = sc->ddl_client->send_balancer_proposal(request);
    std::cout << "send balance proposal result: " << ec.to_string() << std::endl;
    return true;
}

inline bool use_app_as_current(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 1) {
        if (sc->current_app_name.empty()) {
            fprintf(stderr, "Current app not specified.\n");
            return false;
        } else {
            fprintf(stderr, "Current app: %s\n", sc->current_app_name.c_str());
            return true;
        }
    } else if (args.argc == 2) {
        sc->current_app_name = args.argv[1];
        fprintf(stderr, "OK\n");
        return true;
    } else {
        return false;
    }
}

inline bool process_escape_all(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 1) {
        fprintf(stderr, "Current escape_all: %s.\n", sc->escape_all ? "true" : "false");
        return true;
    } else if (args.argc == 2) {
        if (strcmp(args.argv[1], "true") == 0) {
            sc->escape_all = true;
            fprintf(stderr, "OK\n");
            return true;
        } else if (strcmp(args.argv[1], "false") == 0) {
            sc->escape_all = false;
            fprintf(stderr, "OK\n");
            return true;
        } else {
            fprintf(stderr, "ERROR: invalid parameter.\n");
            return false;
        }
    } else {
        return false;
    }
}

inline bool process_timeout(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 1) {
        fprintf(stderr, "Current timeout: %d ms.\n", sc->timeout_ms);
        return true;
    } else if (args.argc == 2) {
        int timeout;
        if (!::pegasus::utils::buf2int(args.argv[1], strlen(args.argv[1]), timeout)) {
            fprintf(stderr, "ERROR: parse %s as timeout failed\n", args.argv[1]);
            return false;
        }
        if (timeout <= 0) {
            fprintf(stderr, "ERROR: invalid timeout %s\n", args.argv[1]);
            return false;
        }
        sc->timeout_ms = timeout;
        fprintf(stderr, "OK\n");
        return true;
    } else {
        return false;
    }
}

inline bool calculate_hash_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }
    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);

    ::dsn::blob key;
    pegasus::pegasus_generate_key(key, hash_key, sort_key);
    uint64_t key_hash = pegasus::pegasus_key_hash(key);

    int width = strlen("partition_index");
    std::cout << std::setw(width) << std::left << "key_hash"
              << " : " << key_hash << std::endl;

    if (!sc->current_app_name.empty()) {
        int32_t app_id;
        int32_t partition_count;
        std::vector<::dsn::partition_configuration> partitions;
        ::dsn::error_code err =
            sc->ddl_client->list_app(sc->current_app_name, app_id, partition_count, partitions);
        if (err != ::dsn::ERR_OK) {
            std::cout << "list app [" << sc->current_app_name
                      << "] failed, error=" << err.to_string() << std::endl;
            return true;
        }
        uint64_t partition_index = key_hash % (uint64_t)partition_count;
        std::cout << std::setw(width) << std::left << "app_name"
                  << " : " << sc->current_app_name << std::endl;
        std::cout << std::setw(width) << std::left << "app_id"
                  << " : " << app_id << std::endl;
        std::cout << std::setw(width) << std::left << "partition_count"
                  << " : " << partition_count << std::endl;
        std::cout << std::setw(width) << std::left << "partition_index"
                  << " : " << partition_index << std::endl;
        if (partitions.size() > partition_index) {
            ::dsn::partition_configuration &pc = partitions[partition_index];
            std::cout << std::setw(width) << std::left << "primary"
                      << " : " << pc.primary.to_string() << std::endl;
            std::ostringstream oss;
            for (int i = 0; i < pc.secondaries.size(); ++i) {
                if (i != 0)
                    oss << ",";
                oss << pc.secondaries[i].to_string();
            }
            std::cout << std::setw(width) << std::left << "secondaries"
                      << " : " << oss.str() << std::endl;
        }
    }
    return true;
}

inline std::string unescape_str(const char *escaped)
{
    std::string dst, src = escaped;
    dassert(pegasus::utils::c_unescape_string(src, dst) >= 0, "");
    return dst;
}

// getopt_long cannot parse argv[i] when it contains '\0' in the middle.
// For "bb\x00cc", getopt_long will parse it as "bb", since getopt_long is not binary-safe.
inline void escape_sds_argv(int argc, sds *argv)
{
    for (int i = 0; i < argc; i++) {
        const size_t dest_len = sdslen(argv[i]) * 4 + 1; // Maximum possible expansion
        sds new_arg = sdsnewlen("", dest_len);
        pegasus::utils::c_escape_string(argv[i], sdslen(argv[i]), new_arg, dest_len);
        sdsfree(argv[i]);
        argv[i] = new_arg;
    }
}

inline bool get_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    std::string value;

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->get(hash_key, sort_key, value, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        if (ret == pegasus::PERR_NOT_FOUND) {
            fprintf(stderr, "Not found\n");
        } else {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        fprintf(stderr, "\"%s\"\n", pegasus::utils::c_escape_string(value, sc->escape_all).c_str());
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool multi_get_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 2)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::set<std::string> sort_keys;
    if (args.argc > 2) {
        for (int i = 2; i < args.argc; i++) {
            std::string sort_key = sds_to_string(args.argv[i]);
            sort_keys.insert(sort_key);
        }
    }
    std::map<std::string, std::string> kvs;

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->multi_get(hash_key, sort_keys, kvs, -1, -1, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        for (auto &kv : kvs) {
            fprintf(stderr,
                    "\"%s\" : \"%s\" => \"%s\"\n",
                    pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(kv.first, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(kv.second, sc->escape_all).c_str());
        }
        fprintf(stderr,
                "\n%d key-value pairs got, fetch %s.\n",
                (int)kvs.size(),
                ret == pegasus::PERR_INCOMPLETE ? "not completed" : "completed");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool multi_get_range(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string start_sort_key = sds_to_string(args.argv[2]);
    std::string stop_sort_key = sds_to_string(args.argv[3]);
    pegasus::pegasus_client::multi_get_options options;
    std::string sort_key_filter_type_name("no_filter");
    int max_count = -1;

    static struct option long_options[] = {{"start_inclusive", required_argument, 0, 'a'},
                                           {"stop_inclusive", required_argument, 0, 'b'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"max_count", required_argument, 0, 'n'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {"reverse", no_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:b:s:y:n:ir", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            if (!::pegasus::utils::buf2bool(optarg, strlen(optarg), options.start_inclusive)) {
                fprintf(stderr, "invalid start_inclusive param\n");
                return false;
            }
            break;
        case 'b':
            if (!::pegasus::utils::buf2bool(optarg, strlen(optarg), options.stop_inclusive)) {
                fprintf(stderr, "invalid stop_inclusive param\n");
                return false;
            }
            break;
        case 's':
            if (!buf2filter_type(optarg, strlen(optarg), options.sort_key_filter_type)) {
                fprintf(stderr, "invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'n':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_count)) {
                fprintf(stderr, "parse %s as max_count failed\n", optarg);
                return false;
            }
            break;
        case 'i':
            options.no_value = true;
            break;
        case 'r':
            options.reverse = true;
            break;
        default:
            return false;
        }
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "start_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(start_sort_key).c_str());
    fprintf(stderr, "start_inclusive: %s\n", options.start_inclusive ? "true" : "false");
    fprintf(
        stderr, "stop_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(stop_sort_key).c_str());
    fprintf(stderr, "stop_inclusive: %s\n", options.stop_inclusive ? "true" : "false");
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "max_count: %d\n", max_count);
    fprintf(stderr, "no_value: %s\n", options.no_value ? "true" : "false");
    fprintf(stderr, "reverse: %s\n", options.reverse ? "true" : "false");
    fprintf(stderr, "\n");

    std::map<std::string, std::string> kvs;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->multi_get(hash_key,
                                       start_sort_key,
                                       stop_sort_key,
                                       options,
                                       kvs,
                                       max_count,
                                       -1,
                                       sc->timeout_ms,
                                       &info);
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        for (auto &kv : kvs) {
            fprintf(stderr,
                    "\"%s\" : \"%s\"",
                    pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(kv.first, sc->escape_all).c_str());
            if (!options.no_value) {
                fprintf(stderr,
                        " => \"%s\"",
                        pegasus::utils::c_escape_string(kv.second, sc->escape_all).c_str());
            }
            fprintf(stderr, "\n");
        }
        if (kvs.size() > 0) {
            fprintf(stderr, "\n");
        }
        fprintf(stderr,
                "%d key-value pairs got, fetch %s.\n",
                (int)kvs.size(),
                ret == pegasus::PERR_INCOMPLETE ? "not completed" : "completed");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool multi_get_sortkeys(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 2)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::set<std::string> sort_keys;

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->multi_get_sortkeys(hash_key, sort_keys, -1, -1, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        for (auto &sort_key : sort_keys) {
            fprintf(stderr,
                    "\"%s\"\n",
                    pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
        }
        fprintf(stderr,
                "\n%d sort keys got, fetch %s.\n",
                (int)sort_keys.size(),
                ret == pegasus::PERR_INCOMPLETE ? "not completed" : "completed");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool exist(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->exist(hash_key, sort_key, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        if (ret == pegasus::PERR_NOT_FOUND) {
            fprintf(stderr, "False\n");
        } else {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        fprintf(stderr, "True\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool sortkey_count(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 2) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    int64_t count;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->sortkey_count(hash_key, count, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "%" PRId64 "\n", count);
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool set_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 4 && args.argc != 5) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    std::string value = sds_to_string(args.argv[3]);
    int32_t ttl = 0;
    if (args.argc == 5) {
        if (!::pegasus::utils::buf2int(args.argv[4], strlen(args.argv[4]), ttl)) {
            fprintf(stderr, "ERROR: parse %s as ttl failed\n", args.argv[4]);
            return false;
        }
        if (ttl <= 0) {
            fprintf(stderr, "ERROR: invalid ttl %s\n", args.argv[4]);
            return false;
        }
    }

    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->set(hash_key, sort_key, value, sc->timeout_ms, ttl, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "OK\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool multi_set_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4 || args.argc % 2 != 0) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::map<std::string, std::string> kvs;
    for (int i = 2; i < args.argc; i += 2) {
        std::string sort_key = sds_to_string(args.argv[i]);
        if (kvs.find(sort_key) != kvs.end()) {
            fprintf(stderr, "ERROR: duplicate sort key %s\n", sort_key.c_str());
            return true;
        }
        std::string value = sds_to_string(args.argv[i + 1]);
        kvs.emplace(std::move(sort_key), std::move(value));
    }
    pegasus::pegasus_client::internal_info info;

    int ret = sc->pg_client->multi_set(hash_key, kvs, sc->timeout_ms, 0, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "OK\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool delete_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->del(hash_key, sort_key, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    } else {
        fprintf(stderr, "OK\n");
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool multi_del_value(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 3)
        return false;
    std::string hash_key = sds_to_string(args.argv[1]);
    std::set<std::string> sort_keys;
    for (int i = 2; i < args.argc; i++) {
        std::string sort_key = sds_to_string(args.argv[i]);
        sort_keys.insert(sort_key);
    }

    pegasus::pegasus_client::internal_info info;
    int64_t deleted_count;
    int ret = sc->pg_client->multi_del(hash_key, sort_keys, deleted_count, sc->timeout_ms, &info);
    if (ret == pegasus::PERR_OK) {
        fprintf(stderr, "%" PRId64 " key-value pairs deleted.\n", deleted_count);
    } else if (ret == pegasus::PERR_INCOMPLETE) {
        fprintf(stderr, "%" PRId64 " key-value pairs deleted, but not completed.\n", deleted_count);
    } else {
        fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "decree          : %ld\n", info.decree);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool multi_del_range(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string start_sort_key = sds_to_string(args.argv[2]);
    std::string stop_sort_key = sds_to_string(args.argv[3]);
    pegasus::pegasus_client::scan_options options;
    options.no_value = true;
    options.timeout_ms = sc->timeout_ms;
    std::string sort_key_filter_type_name("no_filter");
    bool silent = false;
    FILE *file = stderr;
    int batch_del_count = 100;

    static struct option long_options[] = {{"start_inclusive", required_argument, 0, 'a'},
                                           {"stop_inclusive", required_argument, 0, 'b'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"output", required_argument, 0, 'o'},
                                           {"silent", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:b:s:y:o:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            if (!::pegasus::utils::buf2bool(optarg, strlen(optarg), options.start_inclusive)) {
                fprintf(stderr, "invalid start_inclusive param\n");
                return false;
            }
            break;
        case 'b':
            if (!::pegasus::utils::buf2bool(optarg, strlen(optarg), options.stop_inclusive)) {
                fprintf(stderr, "invalid stop_inclusive param\n");
                return false;
            }
            break;
        case 's':
            if (!buf2filter_type(optarg, strlen(optarg), options.sort_key_filter_type)) {
                fprintf(stderr, "invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'o':
            file = fopen(optarg, "w");
            if (!file) {
                fprintf(stderr, "open filename %s failed\n", optarg);
                return false;
            }
            break;
        case 'i':
            silent = true;
            break;
        default:
            return false;
        }
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "start_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(start_sort_key).c_str());
    fprintf(stderr, "start_inclusive: %s\n", options.start_inclusive ? "true" : "false");
    fprintf(
        stderr, "stop_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(stop_sort_key).c_str());
    fprintf(stderr, "stop_inclusive: %s\n", options.stop_inclusive ? "true" : "false");
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "silent: %s\n", silent ? "true" : "false");
    fprintf(stderr, "\n");

    int count = 0;
    bool error_occured = false;
    pegasus::pegasus_client::pegasus_scanner *scanner = nullptr;
    int ret = sc->pg_client->get_scanner(hash_key, start_sort_key, stop_sort_key, options, scanner);
    if (ret != pegasus::PERR_OK) {
        fprintf(file, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        if (file != stderr) {
            fprintf(
                stderr, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        }
        error_occured = true;
    } else {
        std::string tmp_hash_key;
        std::string sort_key;
        std::string value;
        pegasus::pegasus_client::internal_info info;
        std::set<std::string> sort_keys;
        while (true) {
            int scan_ret = scanner->next(tmp_hash_key, sort_key, value, &info);
            if (scan_ret != pegasus::PERR_SCAN_COMPLETE && scan_ret != pegasus::PERR_OK) {
                fprintf(file,
                        "ERROR: scan data failed: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(scan_ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
                if (file != stderr) {
                    fprintf(
                        stderr,
                        "ERROR: scan data failed: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(scan_ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
                }
                error_occured = true;
                break;
            }

            if (scan_ret == pegasus::PERR_OK) {
                sort_keys.emplace(std::move(sort_key));
            }

            if (sort_keys.size() > 0 &&
                (sort_keys.size() >= batch_del_count || scan_ret == pegasus::PERR_SCAN_COMPLETE)) {
                int64_t del_count;
                pegasus::pegasus_client::internal_info del_info;
                int del_ret = sc->pg_client->multi_del(
                    hash_key, sort_keys, del_count, sc->timeout_ms, &del_info);
                if (del_ret != pegasus::PERR_OK) {
                    fprintf(file,
                            "ERROR: delete data failed: %s {app_id=%d, partition_index=%d, "
                            "server=%s}\n",
                            sc->pg_client->get_error_string(del_ret),
                            del_info.app_id,
                            del_info.partition_index,
                            del_info.server.c_str());
                    if (file != stderr) {
                        fprintf(stderr,
                                "ERROR: delete data failed: %s {app_id=%d, partition_index=%d, "
                                "server=%s}\n",
                                sc->pg_client->get_error_string(del_ret),
                                del_info.app_id,
                                del_info.partition_index,
                                del_info.server.c_str());
                    }
                    error_occured = true;
                    break;
                } else {
                    count += del_count;
                    if (!silent) {
                        for (auto &k : sort_keys) {
                            fprintf(file,
                                    "Deleted: \"%s\"\n",
                                    pegasus::utils::c_escape_string(k, sc->escape_all).c_str());
                        }
                    }
                    sort_keys.clear();
                }
            }

            if (scan_ret == pegasus::PERR_SCAN_COMPLETE) {
                break;
            }
        }
    }

    if (scanner) {
        delete scanner;
    }

    if (file != stderr) {
        fclose(file);
    }

    if (error_occured) {
        fprintf(stderr, "\nTerminated for error, %d sort keys deleted.\n", count);
    } else {
        if (file == stderr && !silent && count > 0) {
            fprintf(stderr, "\n");
        }
        fprintf(stderr, "OK, %d sort keys deleted.\n", count);
    }
    return true;
}

inline bool get_ttl(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc != 3) {
        return false;
    }

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string sort_key = sds_to_string(args.argv[2]);
    int ttl_seconds;
    pegasus::pegasus_client::internal_info info;
    int ret = sc->pg_client->ttl(hash_key, sort_key, ttl_seconds, sc->timeout_ms, &info);
    if (ret != pegasus::PERR_OK) {
        if (ret == pegasus::PERR_NOT_FOUND) {
            fprintf(stderr, "Not found\n");
        } else {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        if (ttl_seconds == -1) {
            fprintf(stderr, "Infinite\n");
        } else if (ttl_seconds == -2) {
            fprintf(stderr, "Not found\n");
        } else {
            fprintf(stderr, "%d\n", ttl_seconds);
        }
    }

    fprintf(stderr, "\n");
    fprintf(stderr, "app_id          : %d\n", info.app_id);
    fprintf(stderr, "partition_index : %d\n", info.partition_index);
    fprintf(stderr, "server          : %s\n", info.server.c_str());
    return true;
}

inline bool hash_scan(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 4)
        return false;

    std::string hash_key = sds_to_string(args.argv[1]);
    std::string start_sort_key = sds_to_string(args.argv[2]);
    std::string stop_sort_key = sds_to_string(args.argv[3]);

    int32_t max_count = -1;
    bool detailed = false;
    FILE *file = stderr;
    int32_t timeout_ms = sc->timeout_ms;
    std::string sort_key_filter_type_name("no_filter");
    pegasus::pegasus_client::scan_options options;

    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"max_count", required_argument, 0, 'n'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"output", required_argument, 0, 'o'},
                                           {"start_inclusive", required_argument, 0, 'a'},
                                           {"stop_inclusive", required_argument, 0, 'b'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "dn:t:o:a:b:s:y:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'n':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_count)) {
                fprintf(stderr, "parse %s as max_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), timeout_ms)) {
                fprintf(stderr, "parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'o':
            file = fopen(optarg, "w");
            if (!file) {
                fprintf(stderr, "open filename %s failed\n", optarg);
                return false;
            }
            break;
        case 'a':
            if (!::pegasus::utils::buf2bool(optarg, strlen(optarg), options.start_inclusive)) {
                fprintf(stderr, "invalid start_inclusive param\n");
                return false;
            }
            break;
        case 'b':
            if (!::pegasus::utils::buf2bool(optarg, strlen(optarg), options.stop_inclusive)) {
                fprintf(stderr, "invalid stop_inclusive param\n");
                return false;
            }
            break;
        case 's':
            if (!buf2filter_type(optarg, strlen(optarg), options.sort_key_filter_type)) {
                fprintf(stderr, "invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'i':
            options.no_value = true;
            break;
        default:
            return false;
        }
    }

    fprintf(stderr, "hash_key: \"%s\"\n", pegasus::utils::c_escape_string(hash_key).c_str());
    fprintf(stderr,
            "start_sort_key: \"%s\"\n",
            pegasus::utils::c_escape_string(start_sort_key).c_str());
    fprintf(stderr, "start_inclusive: %s\n", options.start_inclusive ? "true" : "false");
    fprintf(
        stderr, "stop_sort_key: \"%s\"\n", pegasus::utils::c_escape_string(stop_sort_key).c_str());
    fprintf(stderr, "stop_inclusive: %s\n", options.stop_inclusive ? "true" : "false");
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "max_count: %d\n", max_count);
    fprintf(stderr, "no_value: %s\n", options.no_value ? "true" : "false");
    fprintf(stderr, "\n");

    int i = 0;
    pegasus::pegasus_client::pegasus_scanner *scanner = nullptr;
    options.timeout_ms = timeout_ms;
    int ret = sc->pg_client->get_scanner(hash_key, start_sort_key, stop_sort_key, options, scanner);
    if (ret != pegasus::PERR_OK) {
        fprintf(file, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        if (file != stderr) {
            fprintf(
                stderr, "ERROR: get scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else {
        std::string hash_key;
        std::string sort_key;
        std::string value;
        pegasus::pegasus_client::internal_info info;
        for (; (max_count <= 0 || i < max_count) &&
               !(ret = scanner->next(hash_key, sort_key, value, &info));
             i++) {
            fprintf(file,
                    "\"%s\" : \"%s\"",
                    pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                    pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
            if (!options.no_value) {
                fprintf(file,
                        " => \"%s\"",
                        pegasus::utils::c_escape_string(value, sc->escape_all).c_str());
            }
            if (detailed) {
                fprintf(file,
                        " {app_id=%d,partition_index=%d, server=%s}",
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
            }
            fprintf(file, "\n");
        }
        if (ret != pegasus::PERR_SCAN_COMPLETE && ret != pegasus::PERR_OK) {
            fprintf(file,
                    "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                    sc->pg_client->get_error_string(ret),
                    info.app_id,
                    info.partition_index,
                    info.server.c_str());
            if (file != stderr) {
                fprintf(stderr,
                        "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
            }
        }
    }

    if (scanner) {
        delete scanner;
    }

    if (file != stderr) {
        fclose(file);
    }

    if (file == stderr && i > 0) {
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "%d key-value pairs got.\n", i);
    return true;
}

inline bool full_scan(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"max_count", required_argument, 0, 'n'},
                                           {"partition", required_argument, 0, 'p'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"output", required_argument, 0, 'o'},
                                           {"hash_key_filter_type", required_argument, 0, 'h'},
                                           {"hash_key_filter_pattern", required_argument, 0, 'x'},
                                           {"sort_key_filter_type", required_argument, 0, 's'},
                                           {"sort_key_filter_pattern", required_argument, 0, 'y'},
                                           {"no_value", no_argument, 0, 'i'},
                                           {0, 0, 0, 0}};

    int32_t max_count = 0x7FFFFFFF;
    bool detailed = false;
    FILE *file = stderr;
    int32_t timeout_ms = sc->timeout_ms;
    int32_t partition = -1;
    std::string hash_key_filter_type_name("no_filter");
    std::string sort_key_filter_type_name("no_filter");
    pegasus::pegasus_client::scan_options options;

    escape_sds_argv(args.argc, args.argv);
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "dn:p:t:o:h:x:s:y:i", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'n':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_count)) {
                fprintf(stderr, "parse %s as max_count failed\n", optarg);
                return false;
            }
            break;
        case 'p':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), partition)) {
                fprintf(stderr, "parse %s as partition id failed\n", optarg);
                return false;
            }
            if (partition < 0) {
                fprintf(stderr, "invalid partition param, should > 0\n");
                return false;
            }
            break;
        case 't':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), timeout_ms)) {
                fprintf(stderr, "parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'o':
            file = fopen(optarg, "w");
            if (!file) {
                fprintf(stderr, "open filename %s failed\n", optarg);
                return false;
            }
            break;
        case 'h':
            if (!buf2filter_type(optarg, strlen(optarg), options.hash_key_filter_type)) {
                fprintf(stderr, "invalid hash_key_filter_type param\n");
                return false;
            }
            hash_key_filter_type_name = optarg;
            break;
        case 'x':
            options.hash_key_filter_pattern = unescape_str(optarg);
            break;
        case 's':
            if (!buf2filter_type(optarg, strlen(optarg), options.sort_key_filter_type)) {
                fprintf(stderr, "invalid sort_key_filter_type param\n");
                return false;
            }
            sort_key_filter_type_name = optarg;
            break;
        case 'y':
            options.sort_key_filter_pattern = unescape_str(optarg);
            break;
        case 'i':
            options.no_value = true;
            break;
        default:
            return false;
        }
    }

    fprintf(stderr,
            "partition: %s\n",
            partition >= 0 ? boost::lexical_cast<std::string>(partition).c_str() : "all");
    fprintf(stderr, "hash_key_filter_type: %s\n", hash_key_filter_type_name.c_str());
    if (options.hash_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "hash_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.hash_key_filter_pattern).c_str());
    }
    fprintf(stderr, "sort_key_filter_type: %s\n", sort_key_filter_type_name.c_str());
    if (options.sort_key_filter_type != pegasus::pegasus_client::FT_NO_FILTER) {
        fprintf(stderr,
                "sort_key_filter_pattern: \"%s\"\n",
                pegasus::utils::c_escape_string(options.sort_key_filter_pattern).c_str());
    }
    fprintf(stderr, "max_count: %d\n", max_count);
    fprintf(stderr, "no_value: %s\n", options.no_value ? "true" : "false");
    fprintf(stderr, "\n");

    int i = 0;
    std::vector<pegasus::pegasus_client::pegasus_scanner *> scanners;
    options.timeout_ms = timeout_ms;
    int ret = sc->pg_client->get_unordered_scanners(10000, options, scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(file, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        if (file != stderr) {
            fprintf(stderr, "ERROR: %s\n", sc->pg_client->get_error_string(ret));
        }
    } else if (partition >= 0 && partition >= (int)scanners.size()) {
        fprintf(file,
                "ERROR: partition %d out of range, should be in range of [0,%d]\n",
                partition,
                (int)scanners.size() - 1);
        if (file != stderr) {
            fprintf(stderr,
                    "ERROR: partition %d out of range, should be in range of [0,%d]\n",
                    partition,
                    (int)scanners.size() - 1);
        }
    } else {
        for (int j = 0; j < scanners.size(); j++) {
            if (partition >= 0 && partition != j)
                continue;
            std::string hash_key;
            std::string sort_key;
            std::string value;
            pegasus::pegasus_client::internal_info info;
            pegasus::pegasus_client::pegasus_scanner *scanner = scanners[j];
            for (; (max_count <= 0 || i < max_count) &&
                   !(ret = scanner->next(hash_key, sort_key, value, &info));
                 i++) {
                fprintf(file,
                        "\"%s\" : \"%s\"",
                        pegasus::utils::c_escape_string(hash_key, sc->escape_all).c_str(),
                        pegasus::utils::c_escape_string(sort_key, sc->escape_all).c_str());
                if (!options.no_value) {
                    fprintf(file,
                            " => \"%s\"",
                            pegasus::utils::c_escape_string(value, sc->escape_all).c_str());
                }
                if (detailed) {
                    fprintf(file,
                            " {app_id=%d,partition_index=%d, server=%s}",
                            info.app_id,
                            info.partition_index,
                            info.server.c_str());
                }
                fprintf(file, "\n");
            }
            if (ret != pegasus::PERR_SCAN_COMPLETE && ret != pegasus::PERR_OK) {
                fprintf(file,
                        "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                        sc->pg_client->get_error_string(ret),
                        info.app_id,
                        info.partition_index,
                        info.server.c_str());
                if (file != stderr) {
                    fprintf(stderr,
                            "ERROR: %s {app_id=%d, partition_index=%d, server=%s}\n",
                            sc->pg_client->get_error_string(ret),
                            info.app_id,
                            info.partition_index,
                            info.server.c_str());
                }
            }
        }
    }

    for (auto scanner : scanners) {
        delete scanner;
    }

    if (file != stderr) {
        fclose(file);
    }

    if (file == stderr && i > 0) {
        fprintf(stderr, "\n");
    }
    fprintf(stderr, "%d key-value pairs got.\n", i);
    return true;
}

inline bool copy_data(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"target_cluster_name", required_argument, 0, 'c'},
                                           {"target_app_name", required_argument, 0, 'a'},
                                           {"max_split_count", required_argument, 0, 's'},
                                           {"max_batch_count", required_argument, 0, 'b'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {0, 0, 0, 0}};

    std::string target_cluster_name;
    std::string target_app_name;
    int max_split_count = 100000000;
    int max_batch_count = 500;
    int timeout_ms = sc->timeout_ms;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "c:a:s:b:t:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'c':
            target_cluster_name = optarg;
            break;
        case 'a':
            target_app_name = optarg;
            break;
        case 's':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_split_count)) {
                fprintf(stderr, "parse %s as max_split_count failed\n", optarg);
                return false;
            }
            break;
        case 'b':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_batch_count)) {
                fprintf(stderr, "parse %s as max_batch_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), timeout_ms)) {
                fprintf(stderr, "parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    if (target_cluster_name.empty()) {
        fprintf(stderr, "ERROR: target_cluster_name not specified\n");
        return false;
    }

    if (target_app_name.empty()) {
        fprintf(stderr, "ERROR: target_app_name not specified\n");
        return false;
    }

    if (max_split_count <= 0) {
        fprintf(stderr, "ERROR: max_split_count should no less than 0\n");
        return false;
    }

    if (max_batch_count <= 0) {
        fprintf(stderr, "ERROR: max_batch_count should no less than 0\n");
        return false;
    }

    if (timeout_ms <= 0) {
        fprintf(stderr, "ERROR: timeout_ms should no less than 0\n");
        return false;
    }

    fprintf(stderr, "INFO: source_cluster_name = %s\n", sc->pg_client->get_cluster_name());
    fprintf(stderr, "INFO: source_app_name = %s\n", sc->pg_client->get_app_name());
    fprintf(stderr, "INFO: target_cluster_name = %s\n", target_cluster_name.c_str());
    fprintf(stderr, "INFO: target_app_name = %s\n", target_app_name.c_str());
    fprintf(stderr, "INFO: max_split_count = %d\n", max_split_count);
    fprintf(stderr, "INFO: max_batch_count = %d\n", max_batch_count);
    fprintf(stderr, "INFO: timeout_ms = %d\n", timeout_ms);

    if (target_cluster_name == sc->pg_client->get_cluster_name() &&
        target_app_name == sc->pg_client->get_app_name()) {
        fprintf(stderr, "ERROR: source app and target app is the same\n");
        return true;
    }

    pegasus::pegasus_client *target_client = pegasus::pegasus_client_factory::get_client(
        target_cluster_name.c_str(), target_app_name.c_str());
    if (target_client == NULL) {
        fprintf(stderr, "ERROR: get target app client failed\n");
        return true;
    }

    int ret = target_client->exist("a", "b");
    if (ret != pegasus::PERR_OK && ret != pegasus::PERR_NOT_FOUND) {
        fprintf(
            stderr, "ERROR: test target app failed: %s\n", target_client->get_error_string(ret));
        return true;
    }

    std::vector<pegasus::pegasus_client::pegasus_scanner *> scanners;
    pegasus::pegasus_client::scan_options options;
    options.timeout_ms = timeout_ms;
    ret = sc->pg_client->get_unordered_scanners(max_split_count, options, scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(stderr,
                "ERROR: open source app scanner failed: %s\n",
                sc->pg_client->get_error_string(ret));
        return true;
    }
    int split_count = scanners.size();
    fprintf(stderr, "INFO: open source app scanner succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<scan_data_context *> contexts;
    for (int i = 0; i < scanners.size(); i++) {
        scan_data_context *context = new scan_data_context(SCAN_COPY,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i]->get_smart_wrapper(),
                                                           target_client,
                                                           &error_occurred);
        contexts.push_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_data_next, context));
    }

    // wait thread complete
    int sleep_seconds = 0;
    long last_total_rows = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        int completed_split_count = 0;
        long cur_total_rows = 0;
        for (int i = 0; i < scanners.size(); i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            if (contexts[i]->split_request_count.load() == 0)
                completed_split_count++;
        }
        if (error_occurred.load()) {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows, error occurred, terminating...\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        } else {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        }
        if (completed_split_count == scanners.size())
            break;
        last_total_rows = cur_total_rows;
    }

    if (error_occurred.load()) {
        fprintf(stderr, "ERROR: error occurred, processing terminated\n");
    }

    long total_rows = 0;
    for (int i = 0; i < scanners.size(); i++) {
        fprintf(stderr, "INFO: split[%d]: %ld rows\n", i, contexts[i]->split_rows.load());
        total_rows += contexts[i]->split_rows.load();
    }

    for (int i = 0; i < scanners.size(); i++) {
        delete contexts[i];
    }
    contexts.clear();

    fprintf(stderr,
            "\nCopy %s, total %ld rows.\n",
            error_occurred.load() ? "terminated" : "done",
            total_rows);

    return true;
}

inline bool clear_data(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"force", no_argument, 0, 'f'},
                                           {"max_split_count", required_argument, 0, 's'},
                                           {"max_batch_count", required_argument, 0, 'b'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {0, 0, 0, 0}};

    bool force = false;
    int max_split_count = 100000000;
    int max_batch_count = 500;
    int timeout_ms = sc->timeout_ms;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "fs:b:t:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'f':
            force = true;
            break;
        case 's':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_split_count)) {
                fprintf(stderr, "parse %s as max_split_count failed\n", optarg);
                return false;
            }
            break;
        case 'b':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_batch_count)) {
                fprintf(stderr, "parse %s as max_batch_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), timeout_ms)) {
                fprintf(stderr, "parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    if (max_split_count <= 0) {
        fprintf(stderr, "ERROR: max_split_count should no less than 0\n");
        return false;
    }

    if (max_batch_count <= 0) {
        fprintf(stderr, "ERROR: max_batch_count should no less than 0\n");
        return false;
    }

    if (timeout_ms <= 0) {
        fprintf(stderr, "ERROR: timeout_ms should no less than 0\n");
        return false;
    }

    fprintf(stderr, "INFO: cluster_name = %s\n", sc->pg_client->get_cluster_name());
    fprintf(stderr, "INFO: app_name = %s\n", sc->pg_client->get_app_name());
    fprintf(stderr, "INFO: max_split_count = %d\n", max_split_count);
    fprintf(stderr, "INFO: max_batch_count = %d\n", max_batch_count);
    fprintf(stderr, "INFO: timeout_ms = %d\n", timeout_ms);

    if (!force) {
        fprintf(stderr,
                "ERROR: be careful to clear data!!! Please specify --force if you are "
                "determined to do.\n");
        return false;
    }

    std::vector<pegasus::pegasus_client::pegasus_scanner *> scanners;
    pegasus::pegasus_client::scan_options options;
    options.timeout_ms = timeout_ms;
    options.no_value = true;
    int ret = sc->pg_client->get_unordered_scanners(max_split_count, options, scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(
            stderr, "ERROR: open app scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        return true;
    }
    int split_count = scanners.size();
    fprintf(stderr, "INFO: open app scanner succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<scan_data_context *> contexts;
    for (int i = 0; i < scanners.size(); i++) {
        scan_data_context *context = new scan_data_context(SCAN_CLEAR,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i]->get_smart_wrapper(),
                                                           sc->pg_client,
                                                           &error_occurred);
        contexts.push_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_data_next, context));
    }

    int sleep_seconds = 0;
    long last_total_rows = 0;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        int completed_split_count = 0;
        long cur_total_rows = 0;
        for (int i = 0; i < scanners.size(); i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            if (contexts[i]->split_request_count.load() == 0)
                completed_split_count++;
        }
        if (error_occurred.load()) {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows, error occurred, terminating...\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        } else {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        }
        if (completed_split_count == scanners.size())
            break;
        last_total_rows = cur_total_rows;
    }

    if (error_occurred.load()) {
        fprintf(stderr, "ERROR: error occurred, terminate processing\n");
    }

    long total_rows = 0;
    for (int i = 0; i < scanners.size(); i++) {
        fprintf(stderr, "INFO: split[%d]: %ld rows\n", i, contexts[i]->split_rows.load());
        total_rows += contexts[i]->split_rows.load();
    }

    for (int i = 0; i < scanners.size(); i++) {
        delete contexts[i];
    }
    contexts.clear();

    fprintf(stderr,
            "\nClear %s, total %ld rows.\n",
            error_occurred.load() ? "terminated" : "done",
            total_rows);

    return true;
}

inline bool count_data(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"max_split_count", required_argument, 0, 's'},
                                           {"max_batch_count", required_argument, 0, 'b'},
                                           {"timeout_ms", required_argument, 0, 't'},
                                           {"stat_size", no_argument, 0, 'z'},
                                           {"top_count", required_argument, 0, 'c'},
                                           {"run_seconds", required_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    int max_split_count = 100000000;
    int max_batch_count = 500;
    int timeout_ms = sc->timeout_ms;
    bool stat_size = false;
    int top_count = 0;
    int run_seconds = 0;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "s:b:t:zc:r:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 's':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_split_count)) {
                fprintf(stderr, "parse %s as max_split_count failed\n", optarg);
                return false;
            }
            break;
        case 'b':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), max_batch_count)) {
                fprintf(stderr, "parse %s as max_batch_count failed\n", optarg);
                return false;
            }
            break;
        case 't':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), timeout_ms)) {
                fprintf(stderr, "parse %s as timeout_ms failed\n", optarg);
                return false;
            }
            break;
        case 'z':
            stat_size = true;
            break;
        case 'c':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), top_count)) {
                fprintf(stderr, "parse %s as top_count failed\n", optarg);
                return false;
            }
            break;
        case 'r':
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), run_seconds)) {
                fprintf(stderr, "parse %s as run_seconds failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    if (max_split_count <= 0) {
        fprintf(stderr, "ERROR: max_split_count should be greater than 0\n");
        return false;
    }

    if (max_batch_count <= 0) {
        fprintf(stderr, "ERROR: max_batch_count should be greater than 0\n");
        return false;
    }

    if (timeout_ms <= 0) {
        fprintf(stderr, "ERROR: timeout_ms should be greater than 0\n");
        return false;
    }

    if (top_count < 0) {
        fprintf(stderr, "ERROR: top_count should be no less than 0\n");
        return false;
    }

    if (run_seconds < 0) {
        fprintf(stderr, "ERROR: run_seconds should be no less than 0\n");
        return false;
    }

    fprintf(stderr, "INFO: cluster_name = %s\n", sc->pg_client->get_cluster_name());
    fprintf(stderr, "INFO: app_name = %s\n", sc->pg_client->get_app_name());
    fprintf(stderr, "INFO: max_split_count = %d\n", max_split_count);
    fprintf(stderr, "INFO: max_batch_count = %d\n", max_batch_count);
    fprintf(stderr, "INFO: timeout_ms = %d\n", timeout_ms);
    fprintf(stderr, "INFO: stat_size = %s\n", stat_size ? "true" : "false");
    fprintf(stderr, "INFO: top_count = %d\n", top_count);
    fprintf(stderr, "INFO: run_seconds = %d\n", run_seconds);

    std::vector<pegasus::pegasus_client::pegasus_scanner *> scanners;
    pegasus::pegasus_client::scan_options options;
    options.timeout_ms = timeout_ms;
    options.no_value = !stat_size;
    int ret = sc->pg_client->get_unordered_scanners(max_split_count, options, scanners);
    if (ret != pegasus::PERR_OK) {
        fprintf(
            stderr, "ERROR: open app scanner failed: %s\n", sc->pg_client->get_error_string(ret));
        return true;
    }
    int split_count = scanners.size();
    fprintf(stderr, "INFO: open app scanner succeed, split_count = %d\n", split_count);

    std::atomic_bool error_occurred(false);
    std::vector<scan_data_context *> contexts;
    for (int i = 0; i < scanners.size(); i++) {
        scan_data_context *context = new scan_data_context(SCAN_COUNT,
                                                           i,
                                                           max_batch_count,
                                                           timeout_ms,
                                                           scanners[i]->get_smart_wrapper(),
                                                           sc->pg_client,
                                                           &error_occurred,
                                                           stat_size,
                                                           top_count);
        contexts.push_back(context);
        dsn::tasking::enqueue(LPC_SCAN_DATA, nullptr, std::bind(scan_data_next, context));
    }

    int sleep_seconds = 0;
    long last_total_rows = 0;
    bool stopped_by_wait_seconds = false;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        sleep_seconds++;
        if (run_seconds > 0 && !stopped_by_wait_seconds && sleep_seconds >= run_seconds) {
            // here use compare-and-swap primitive:
            // - if error_occurred is already set true by scanners as error occured, then
            //   stopped_by_wait_seconds will be assigned as false.
            // - else, error_occurred will be set true, and stopped_by_wait_seconds will be
            //   assigned as true.
            bool expected = false;
            stopped_by_wait_seconds = error_occurred.compare_exchange_strong(expected, true);
        }
        int completed_split_count = 0;
        long cur_total_rows = 0;
        for (int i = 0; i < scanners.size(); i++) {
            cur_total_rows += contexts[i]->split_rows.load();
            if (contexts[i]->split_request_count.load() == 0)
                completed_split_count++;
        }
        if (!stopped_by_wait_seconds && error_occurred.load()) {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows, error occurred, terminating...\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        } else {
            fprintf(stderr,
                    "INFO: processed for %d seconds, (%d/%d) splits, total %ld rows, last second "
                    "%ld rows\n",
                    sleep_seconds,
                    completed_split_count,
                    split_count,
                    cur_total_rows,
                    cur_total_rows - last_total_rows);
        }
        if (completed_split_count == scanners.size())
            break;
        last_total_rows = cur_total_rows;
        if (stat_size && sleep_seconds % 10 == 0) {
            long total_rows = 0;
            long hash_key_size_sum = 0;
            long hash_key_size_max = 0;
            long sort_key_size_sum = 0;
            long sort_key_size_max = 0;
            long value_size_sum = 0;
            long value_size_max = 0;
            long row_size_max = 0;
            for (int i = 0; i < scanners.size(); i++) {
                total_rows += contexts[i]->split_rows.load();
                hash_key_size_sum += contexts[i]->hash_key_size_sum.load();
                hash_key_size_max =
                    std::max(contexts[i]->hash_key_size_max.load(), hash_key_size_max);
                sort_key_size_sum += contexts[i]->sort_key_size_sum.load();
                sort_key_size_max =
                    std::max(contexts[i]->sort_key_size_max.load(), sort_key_size_max);
                value_size_sum += contexts[i]->value_size_sum.load();
                value_size_max = std::max(contexts[i]->value_size_max.load(), value_size_max);
                row_size_max = std::max(contexts[i]->row_size_max.load(), row_size_max);
            }
            long row_size_sum = hash_key_size_sum + sort_key_size_sum + value_size_sum;
            double hash_key_size_avg =
                total_rows == 0 ? 0.0 : (double)hash_key_size_sum / total_rows;
            double sort_key_size_avg =
                total_rows == 0 ? 0.0 : (double)sort_key_size_sum / total_rows;
            double value_size_avg = total_rows == 0 ? 0.0 : (double)value_size_sum / total_rows;
            double row_size_avg = total_rows == 0 ? 0.0 : (double)row_size_sum / total_rows;
            fprintf(stderr, "[row].count = %ld\n", total_rows);
            fprintf(stderr, "[hash_key].size_sum = %ld\n", hash_key_size_sum);
            fprintf(stderr, "[hash_key].size_max = %ld\n", hash_key_size_max);
            fprintf(stderr, "[hash_key].size_avg = %.2f\n", hash_key_size_avg);
            fprintf(stderr, "[sort_key].size_sum = %ld\n", sort_key_size_sum);
            fprintf(stderr, "[sort_key].size_max = %ld\n", sort_key_size_max);
            fprintf(stderr, "[sort_key].size_avg = %.2f\n", sort_key_size_avg);
            fprintf(stderr, "[value].size_sum = %ld\n", value_size_sum);
            fprintf(stderr, "[value].size_max = %ld\n", value_size_max);
            fprintf(stderr, "[value].size_avg = %.2f\n", value_size_avg);
            fprintf(stderr, "[row].size_sum = %ld\n", row_size_sum);
            fprintf(stderr, "[row].size_max = %ld\n", row_size_max);
            fprintf(stderr, "[row].size_avg = %.2f\n", row_size_avg);
        }
    }

    if (error_occurred.load()) {
        if (stopped_by_wait_seconds) {
            fprintf(stderr, "INFO: reached run seconds, terminate processing\n");
        } else {
            fprintf(stderr, "ERROR: error occurred, terminate processing\n");
        }
    }

    long total_rows = 0;
    long hash_key_size_sum = 0;
    long hash_key_size_max = 0;
    long sort_key_size_sum = 0;
    long sort_key_size_max = 0;
    long value_size_sum = 0;
    long value_size_max = 0;
    long row_size_max = 0;
    for (int i = 0; i < scanners.size(); i++) {
        fprintf(stderr, "INFO: split[%d]: %ld rows\n", i, contexts[i]->split_rows.load());
        total_rows += contexts[i]->split_rows.load();
        if (stat_size) {
            hash_key_size_sum += contexts[i]->hash_key_size_sum.load();
            hash_key_size_max = std::max(contexts[i]->hash_key_size_max.load(), hash_key_size_max);
            sort_key_size_sum += contexts[i]->sort_key_size_sum.load();
            sort_key_size_max = std::max(contexts[i]->sort_key_size_max.load(), sort_key_size_max);
            value_size_sum += contexts[i]->value_size_sum.load();
            value_size_max = std::max(contexts[i]->value_size_max.load(), value_size_max);
            row_size_max = std::max(contexts[i]->row_size_max.load(), row_size_max);
        }
    }

    std::string stop_desc;
    if (error_occurred.load()) {
        if (stopped_by_wait_seconds) {
            stop_desc = "terminated as run time used out";
        } else {
            stop_desc = "terminated as error occurred";
        }
    } else {
        stop_desc = "done";
    }
    fprintf(stderr, "\nCount %s, total %ld rows.\n", stop_desc.c_str(), total_rows);

    if (stat_size) {
        long row_size_sum = hash_key_size_sum + sort_key_size_sum + value_size_sum;
        double hash_key_size_avg = total_rows == 0 ? 0.0 : (double)hash_key_size_sum / total_rows;
        double sort_key_size_avg = total_rows == 0 ? 0.0 : (double)sort_key_size_sum / total_rows;
        double value_size_avg = total_rows == 0 ? 0.0 : (double)value_size_sum / total_rows;
        double row_size_avg = total_rows == 0 ? 0.0 : (double)row_size_sum / total_rows;
        fprintf(stderr, "[hash_key].size_sum = %ld\n", hash_key_size_sum);
        fprintf(stderr, "[hash_key].size_max = %ld\n", hash_key_size_max);
        fprintf(stderr, "[hash_key].size_avg = %.2f\n", hash_key_size_avg);
        fprintf(stderr, "[sort_key].size_sum = %ld\n", sort_key_size_sum);
        fprintf(stderr, "[sort_key].size_max = %ld\n", sort_key_size_max);
        fprintf(stderr, "[sort_key].size_avg = %.2f\n", sort_key_size_avg);
        fprintf(stderr, "[value].size_sum = %ld\n", value_size_sum);
        fprintf(stderr, "[value].size_max = %ld\n", value_size_max);
        fprintf(stderr, "[value].size_avg = %.2f\n", value_size_avg);
        fprintf(stderr, "[row].size_sum = %ld\n", row_size_sum);
        fprintf(stderr, "[row].size_max = %ld\n", row_size_max);
        fprintf(stderr, "[row].size_avg = %.2f\n", row_size_avg);
        if (top_count > 0) {
            top_container::top_heap heap;
            for (int i = 0; i < scanners.size(); i++) {
                top_container::top_heap &h = contexts[i]->top_rows.all();
                while (!h.empty()) {
                    heap.push(h.top());
                    h.pop();
                }
            }
            for (int i = 1; i <= top_count && !heap.empty(); i++) {
                const top_container::top_heap_item &item = heap.top();
                fprintf(stderr,
                        "[top][%d].hash_key = \"%s\"\n",
                        i,
                        pegasus::utils::c_escape_string(item.hash_key, sc->escape_all).c_str());
                fprintf(stderr,
                        "[top][%d].sort_key = \"%s\"\n",
                        i,
                        pegasus::utils::c_escape_string(item.sort_key, sc->escape_all).c_str());
                fprintf(stderr, "[top][%d].row_size = %ld\n", i, item.row_size);
                heap.pop();
            }
        }
    }

    for (int i = 0; i < scanners.size(); i++) {
        delete contexts[i];
    }
    contexts.clear();

    return true;
}

inline bool data_operations(command_executor *e, shell_context *sc, arguments args)
{
    static std::map<std::string, executor> data_operations_map = {
        {"set", set_value},
        {"multi_set", multi_set_value},
        {"get", get_value},
        {"multi_get", multi_get_value},
        {"multi_get_range", multi_get_range},
        {"multi_get_sortkeys", multi_get_sortkeys},
        {"del", delete_value},
        {"multi_del", multi_del_value},
        {"multi_del_range", multi_del_range},
        {"exist", exist},
        {"count", sortkey_count},
        {"ttl", get_ttl},
        {"hash_scan", hash_scan},
        {"full_scan", full_scan},
        {"copy_data", copy_data},
        {"clear_data", clear_data},
        {"count_data", count_data}};

    if (args.argc <= 0) {
        return false;
    }

    auto iter = data_operations_map.find(args.argv[0]);
    dassert(iter != data_operations_map.end(), "filter should done earlier");
    executor func = iter->second;

    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    sc->pg_client = pegasus::pegasus_client_factory::get_client(sc->current_cluster_name.c_str(),
                                                                sc->current_app_name.c_str());
    if (sc->pg_client == nullptr) {
        fprintf(stderr,
                "get client error, cluster_name(%s), app_name(%s)\n",
                sc->current_cluster_name.c_str(),
                sc->current_app_name.c_str());
        return true;
    }

    return func(e, sc, args);
}

inline bool local_get(command_executor *e, shell_context *sc, arguments args)
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

inline bool sst_dump(command_executor *e, shell_context *sc, arguments args)
{
    rocksdb::SSTDumpTool tool;
    tool.Run(args.argc, args.argv);
    return true;
}

static const char *INDENT = "  ";
DEFINE_TASK_CODE_RPC(RPC_RRDB_RRDB_INCR, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
inline bool mlog_dump(command_executor *e, shell_context *sc, arguments args)
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

    std::function<void(int64_t decree, int64_t timestamp, dsn_message_t * requests, int count)>
        callback;
    if (detailed) {
        callback = [&os, sc](
            int64_t decree, int64_t timestamp, dsn_message_t *requests, int count) mutable {
            for (int i = 0; i < count; ++i) {
                dsn_message_t request = requests[i];
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
                } else if (msg->local_rpc_code == RPC_RRDB_RRDB_INCR) {
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

inline bool recover(command_executor *e, shell_context *sc, arguments args)
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
            if (!::pegasus::utils::buf2int(optarg, strlen(optarg), wait_seconds)) {
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
        dsn::utils::split_args(args.argv[1], tokens, ',');
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

inline bool remote_command(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"node_type", required_argument, 0, 't'},
                                           {"node_list", required_argument, 0, 'l'},
                                           {0, 0, 0, 0}};

    std::string type;
    std::string nodes;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "t:l:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 't':
            type = optarg;
            break;
        case 'l':
            nodes = optarg;
            break;
        default:
            return false;
        }
    }

    if (!type.empty() && !nodes.empty()) {
        fprintf(stderr, "can not specify both node_type and node_list\n");
        return false;
    }

    if (type.empty() && nodes.empty()) {
        type = "all";
    }

    if (!type.empty() && type != "all" && type != "meta-server" && type != "replica-server") {
        fprintf(stderr, "invalid type, should be: all | meta-server | replica-server\n");
        return false;
    }

    if (optind == args.argc) {
        fprintf(stderr, "command not specified\n");
        return false;
    }

    ::dsn::command cmd;
    cmd.cmd = args.argv[optind];
    for (int i = optind + 1; i < args.argc; i++) {
        cmd.arguments.push_back(args.argv[i]);
    }

    std::vector<node_desc> node_list;
    if (!type.empty()) {
        if (!fill_nodes(sc, type, node_list)) {
            fprintf(stderr, "prepare nodes failed, type = %s\n", type.c_str());
            return true;
        }
    } else {
        std::vector<std::string> tokens;
        dsn::utils::split_args(nodes.c_str(), tokens, ',');
        if (tokens.empty()) {
            fprintf(stderr, "can't parse node from node_list\n");
            return true;
        }

        for (std::string &token : tokens) {
            dsn::rpc_address node;
            if (!node.from_string_ipv4(token.c_str())) {
                fprintf(stderr, "parse %s as a ip:port node failed\n", token.c_str());
                return true;
            }
            node_list.emplace_back("user-specified", node);
        }
    }

    fprintf(stderr, "COMMAND: %s", cmd.cmd.c_str());
    for (auto &s : cmd.arguments) {
        fprintf(stderr, " %s", s.c_str());
    }
    fprintf(stderr, "\n\n");

    std::vector<std::pair<bool, std::string>> results;
    call_remote_command(sc, node_list, cmd, results);

    int succeed = 0;
    int failed = 0;
    for (int i = 0; i < node_list.size(); ++i) {
        node_desc &n = node_list[i];
        fprintf(stderr, "CALL [%s] [%s] ", n.desc.c_str(), n.address.to_string());
        if (results[i].first) {
            fprintf(stderr, "succeed: %s\n", results[i].second.c_str());
            succeed++;
        } else {
            fprintf(stderr, "failed: %s\n", results[i].second.c_str());
            failed++;
        }
    }

    fprintf(stderr, "\nSucceed count: %d\n", succeed);
    fprintf(stderr, "Failed count: %d\n", failed);

    return true;
}

inline bool server_info(command_executor *e, shell_context *sc, arguments args)
{
    char *argv[args.argc + 1];
    memcpy(argv, args.argv, sizeof(char *) * args.argc);
    argv[args.argc] = (char *)"server-info";
    arguments new_args;
    new_args.argc = args.argc + 1;
    new_args.argv = argv;
    return remote_command(e, sc, new_args);
}

inline bool server_stat(command_executor *e, shell_context *sc, arguments args)
{
    char *argv[args.argc + 1];
    memcpy(argv, args.argv, sizeof(char *) * args.argc);
    argv[args.argc] = (char *)"server-stat";
    arguments new_args;
    new_args.argc = args.argc + 1;
    new_args.argv = argv;
    return remote_command(e, sc, new_args);
}

inline bool flush_log(command_executor *e, shell_context *sc, arguments args)
{
    char *argv[args.argc + 1];
    memcpy(argv, args.argv, sizeof(char *) * args.argc);
    argv[args.argc] = (char *)"flush-log";
    arguments new_args;
    new_args.argc = args.argc + 1;
    new_args.argv = argv;
    return remote_command(e, sc, new_args);
}

inline bool app_disk(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    static struct option long_options[] = {
        {"detailed", no_argument, 0, 'd'}, {"output", required_argument, 0, 'o'}, {0, 0, 0, 0}};

    std::string app_name = args.argv[1];
    std::string out_file;
    bool detailed = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "do:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'o':
            out_file = optarg;
            break;
        default:
            return false;
        }
    }

    if (!(app_name.empty() && out_file.empty())) {
        std::cout << "[Parameters]" << std::endl;
        if (!app_name.empty())
            std::cout << "app_name: " << app_name << std::endl;
        if (!out_file.empty())
            std::cout << "out_file: " << out_file << std::endl;
    }
    if (detailed)
        std::cout << "detailed: true" << std::endl;
    else
        std::cout << "detailed: false" << std::endl;
    std::cout << std::endl << "[Result]" << std::endl;

    if (app_name.empty()) {
        std::cout << "ERROR: null app name" << std::endl;
        return false;
    }

    int32_t app_id = 0;
    int32_t partition_count = 0;
    int32_t max_replica_count = 0;
    std::vector<dsn::partition_configuration> partitions;
    dsn::error_code err = sc->ddl_client->list_app(app_name, app_id, partition_count, partitions);
    if (err != ::dsn::ERR_OK) {
        std::cout << "ERROR: list app " << app_name << " failed, error=" << err.to_string()
                  << std::endl;
        return true;
    }
    if (!partitions.empty()) {
        max_replica_count = partitions[0].max_replica_count;
    }

    std::vector<node_desc> nodes;
    if (!fill_nodes(sc, "replica-server", nodes)) {
        std::cout << "ERROR: get replica server node list failed" << std::endl;
        return true;
    }

    ::dsn::command command;
    command.cmd = "perf-counters";
    char tmp[256];
    sprintf(tmp, ".*\\*app\\.pegasus\\*disk\\.storage\\.sst.*@%d\\..*", app_id);
    command.arguments.push_back(tmp);
    std::vector<std::pair<bool, std::string>> results;
    call_remote_command(sc, nodes, command, results);
    std::map<dsn::rpc_address, std::map<int32_t, double>> disk_map;
    std::map<dsn::rpc_address, std::map<int32_t, double>> count_map;
    for (int i = 0; i < nodes.size(); ++i) {
        if (!results[i].first) {
            std::cout << "ERROR: query perf counter from node " << nodes[i].address.to_string()
                      << " failed" << std::endl;
            return true;
        }
        pegasus::perf_counter_info info;
        dsn::blob bb(results[i].second.data(), 0, results[i].second.size());
        if (!dsn::json::json_forwarder<pegasus::perf_counter_info>::decode(bb, info)) {
            std::cout << "ERROR: decode perf counter info from node "
                      << nodes[i].address.to_string() << " failed, result = " << results[i].second
                      << std::endl;
            return true;
        }
        if (info.result != "OK") {
            std::cout << "ERROR: query perf counter info from node " << nodes[i].address.to_string()
                      << " returns error, error = " << info.result << std::endl;
            return true;
        }
        for (pegasus::perf_counter_metric &m : info.counters) {
            int32_t app_id_x, partition_index_x;
            std::string counter_name;
            bool parse_ret = parse_app_pegasus_perf_counter_name(
                m.name, app_id_x, partition_index_x, counter_name);
            dassert(parse_ret, "name = %s", m.name.c_str());
            if (m.name.find("sst(MB)") != std::string::npos) {
                disk_map[nodes[i].address][partition_index_x] = m.value;
            } else if (m.name.find("sst.count") != std::string::npos) {
                count_map[nodes[i].address][partition_index_x] = m.value;
            }
        }
    }

    // print configuration_query_by_index_response
    std::streambuf *buf;
    std::ofstream of;

    if (!out_file.empty()) {
        of.open(out_file);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    int width = strlen("disk_used_for_primary_replicas(MB)");
    out << std::setw(width) << std::left << "app_name"
        << " : " << app_name << std::endl;
    out << std::setw(width) << std::left << "app_id"
        << " : " << app_id << std::endl;
    out << std::setw(width) << std::left << "partition_count"
        << " : " << partition_count << std::endl;
    out << std::setw(width) << std::left << "max_replica_count"
        << " : " << max_replica_count << std::endl;
    if (detailed) {
        out << std::setw(width) << std::left << "details"
            << " : " << std::endl
            << std::setw(10) << std::left << "pidx" << std::setw(10) << std::left << "ballot"
            << std::setw(20) << std::left << "replica_count" << std::setw(40) << std::left
            << "primary" << std::setw(80) << std::left << "secondaries" << std::endl;
    }
    double disk_used_for_primary_replicas = 0;
    int primary_replicas_count = 0;
    double disk_used_for_all_replicas = 0;
    int all_replicas_count = 0;
    for (int i = 0; i < partitions.size(); i++) {
        const dsn::partition_configuration &p = partitions[i];
        int replica_count = 0;
        if (!p.primary.is_invalid()) {
            replica_count++;
        }
        replica_count += p.secondaries.size();
        std::string replica_count_str;
        {
            std::stringstream oss;
            oss << replica_count << "/" << p.max_replica_count;
            replica_count_str = oss.str();
        }
        std::string primary_str("-");
        if (!p.primary.is_invalid()) {
            bool disk_found = false;
            double disk_value = 0;
            auto f1 = disk_map.find(p.primary);
            if (f1 != disk_map.end()) {
                auto &sub_map = f1->second;
                auto f2 = sub_map.find(p.pid.get_partition_index());
                if (f2 != sub_map.end()) {
                    disk_found = true;
                    disk_value = f2->second;
                    disk_used_for_primary_replicas += disk_value;
                    primary_replicas_count++;
                    disk_used_for_all_replicas += disk_value;
                    all_replicas_count++;
                }
            }
            bool count_found = false;
            double count_value = 0;
            auto f3 = count_map.find(p.primary);
            if (f3 != count_map.end()) {
                auto &sub_map = f3->second;
                auto f3 = sub_map.find(p.pid.get_partition_index());
                if (f3 != sub_map.end()) {
                    count_found = true;
                    count_value = f3->second;
                }
            }
            std::stringstream oss;
            oss << p.primary.to_string() << "(";
            if (disk_found)
                oss << disk_value;
            else
                oss << "-";
            oss << ",";
            if (count_found)
                oss << "#" << count_value;
            else
                oss << "-";
            oss << ")";
            primary_str = oss.str();
        }
        std::string secondary_str;
        {
            std::stringstream oss;
            oss << "[";
            for (int j = 0; j < p.secondaries.size(); j++) {
                if (j != 0)
                    oss << ",";
                bool found = false;
                double value = 0;
                auto f1 = disk_map.find(p.secondaries[j]);
                if (f1 != disk_map.end()) {
                    auto &sub_map = f1->second;
                    auto f2 = sub_map.find(p.pid.get_partition_index());
                    if (f2 != sub_map.end()) {
                        found = true;
                        value = f2->second;
                        disk_used_for_all_replicas += value;
                        all_replicas_count++;
                    }
                }
                bool count_found = false;
                double count_value = 0;
                auto f3 = count_map.find(p.secondaries[j]);
                if (f3 != count_map.end()) {
                    auto &sub_map = f3->second;
                    auto f3 = sub_map.find(p.pid.get_partition_index());
                    if (f3 != sub_map.end()) {
                        count_found = true;
                        count_value = f3->second;
                    }
                }
                oss << p.secondaries[j].to_string() << "(";
                if (found)
                    oss << value;
                else
                    oss << "-";
                oss << ",";
                if (count_found)
                    oss << "#" << count_value;
                else
                    oss << "-";
                oss << ")";
            }
            oss << "]";
            secondary_str = oss.str();
        }
        if (detailed) {
            out << std::setw(10) << std::left << p.pid.get_partition_index() << std::setw(10)
                << std::left << p.ballot << std::setw(20) << std::left << replica_count_str
                << std::setw(40) << std::left << primary_str << std::setw(80) << std::left
                << secondary_str << std::endl;
        }
    }
    out << std::setw(width) << std::left << "disk_used_for_primary_replicas(MB)"
        << " : " << disk_used_for_primary_replicas;
    if (primary_replicas_count < partition_count)
        out << " (" << (partition_count - primary_replicas_count) << "/" << partition_count
            << " partitions not counted)";
    out << std::endl;
    out << std::setw(width) << std::left << "disk_used_for_all_replicas(MB)"
        << " : " << disk_used_for_all_replicas;
    if (all_replicas_count < partition_count * max_replica_count)
        out << " (" << (partition_count * max_replica_count - all_replicas_count) << "/"
            << (partition_count * max_replica_count) << " replicas not counted)";
    out << std::endl;
    out << std::endl;
    std::cout << "list disk usage for app " << app_name << " succeed" << std::endl;
    return true;
}

inline bool app_stat(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string app_name;
    std::string out_file;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:o:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'o':
            out_file = optarg;
            break;
        default:
            return false;
        }
    }

    std::vector<row_data> rows;
    if (!get_app_stat(sc, app_name, rows)) {
        return false;
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

    size_t w = 12;
    size_t first_column_width = w;
    if (app_name.empty()) {
        for (row_data &row : rows) {
            first_column_width = std::max(first_column_width, row.row_name.size() + 2);
        }
        out << std::setw(first_column_width) << std::left << "app";
    } else {
        out << std::setw(first_column_width) << std::left << "pidx";
    }
    out << std::setw(w) << std::right << "GET" << std::setw(w) << std::right << "MULTI_GET"
        << std::setw(w) << std::right << "PUT" << std::setw(w) << std::right << "MULTI_PUT"
        << std::setw(w) << std::right << "DEL" << std::setw(w) << std::right << "MULTI_DEL"
        << std::setw(w) << std::right << "SCAN" << std::setw(w) << std::right << "expired"
        << std::setw(w) << std::right << "filtered" << std::setw(w) << std::right << "abnormal"
        << std::setw(w) << std::right << "storage_mb" << std::setw(w) << std::right << "file_count"
        << std::endl;
    rows.resize(rows.size() + 1);
    row_data &sum = rows.back();
    for (int i = 0; i < rows.size() - 1; ++i) {
        row_data &row = rows[i];
        sum.get_qps += row.get_qps;
        sum.multi_get_qps += row.multi_get_qps;
        sum.put_qps += row.put_qps;
        sum.multi_put_qps += row.multi_put_qps;
        sum.remove_qps += row.remove_qps;
        sum.multi_remove_qps += row.multi_remove_qps;
        sum.scan_qps += row.scan_qps;
        sum.recent_expire_count += row.recent_expire_count;
        sum.recent_filter_count += row.recent_filter_count;
        sum.recent_abnormal_count += row.recent_abnormal_count;
        sum.storage_mb += row.storage_mb;
        sum.storage_count += row.storage_count;
    }
#define PRINT_QPS(field)                                                                           \
    do {                                                                                           \
        if (row.field == 0)                                                                        \
            out << std::setw(w) << std::right << 0;                                                \
        else                                                                                       \
            out << std::setw(w) << std::right << row.field;                                        \
    } while (0)
    for (row_data &row : rows) {
        out << std::setw(first_column_width) << std::left << row.row_name << std::fixed
            << std::setprecision(2);
        PRINT_QPS(get_qps);
        PRINT_QPS(multi_get_qps);
        PRINT_QPS(put_qps);
        PRINT_QPS(multi_put_qps);
        PRINT_QPS(remove_qps);
        PRINT_QPS(multi_remove_qps);
        PRINT_QPS(scan_qps);
        out << std::setw(w) << std::right << (int64_t)row.recent_expire_count << std::setw(w)
            << std::right << (int64_t)row.recent_filter_count << std::setw(w) << std::right
            << (int64_t)row.recent_abnormal_count << std::setw(w) << std::right
            << (int64_t)row.storage_mb << std::setw(w) << std::right << (int64_t)row.storage_count
            << std::endl;
    }
#undef PRINT_QPS

    std::cout << std::endl;
    if (app_name.empty())
        std::cout << "list statistics for apps succeed" << std::endl;
    else
        std::cout << "list statistics for app " << app_name << " succeed" << std::endl;
    return true;
}

inline bool restore(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"old_cluster_name", required_argument, 0, 'c'},
                                           {"old_policy_name", required_argument, 0, 'p'},
                                           {"old_app_name", required_argument, 0, 'a'},
                                           {"old_app_id", required_argument, 0, 'i'},
                                           {"new_app_name", required_argument, 0, 'n'},
                                           {"timestamp", required_argument, 0, 't'},
                                           {"backup_provider_type", required_argument, 0, 'b'},
                                           {"skip_bad_partition", no_argument, 0, 's'},
                                           {0, 0, 0, 0}};
    std::string old_cluster_name, old_policy_name;
    std::string old_app_name, new_app_name;
    std::string backup_provider_type;
    int32_t old_app_id = 0;
    int64_t timestamp = 0;
    bool skip_bad_partition = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "c:p:a:i:n:t:b:s", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'c':
            old_cluster_name = optarg;
            break;
        case 'p':
            old_policy_name = optarg;
            break;
        case 'a':
            old_app_name = optarg;
            break;
        case 'i':
            old_app_id = boost::lexical_cast<int32_t>(optarg);
            break;
        case 'n':
            new_app_name = optarg;
            break;
        case 't':
            timestamp = boost::lexical_cast<int64_t>(optarg);
            break;
        case 'b':
            backup_provider_type = optarg;
            break;
        case 's':
            skip_bad_partition = true;
            break;
        default:
            fprintf(stderr, "invalid parameter\n");
            return false;
        }
    }

    if (old_cluster_name.empty() || old_policy_name.empty() || old_app_name.empty() ||
        old_app_id <= 0 || timestamp <= 0 || backup_provider_type.empty()) {
        fprintf(stderr, "invalid parameter\n");
        return false;
    }

    if (new_app_name.empty()) {
        // using old_app_name
        new_app_name = old_app_name;
    }

    ::dsn::error_code err = sc->ddl_client->do_restore(backup_provider_type,
                                                       old_cluster_name,
                                                       old_policy_name,
                                                       timestamp,
                                                       old_app_name,
                                                       old_app_id,
                                                       new_app_name,
                                                       skip_bad_partition);
    if (err != ::dsn::ERR_OK) {
        fprintf(stderr, "restore app failed with err(%s)\n", err.to_string());
    }
    return true;
}

inline bool query_restore_status(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 2) {
        fprintf(stderr, "invalid parameter\n");
        return false;
    }

    int32_t restore_app_id = boost::lexical_cast<int32_t>(args.argv[1]);
    if (restore_app_id <= 0) {
        fprintf(stderr, "invalid restore_app_id(%d)\n", restore_app_id);
        return false;
    }
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'}, {0, 0, 0, 0}};
    optind = 0;
    bool detailed = false;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "d", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        default:
            return false;
        }
    }
    ::dsn::error_code ret = sc->ddl_client->query_restore(restore_app_id, detailed);

    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr,
                "query restore status failed, restore_app_id(%d), err = %s\n",
                restore_app_id,
                ret.to_string());
    }
    return true;
}

inline bool add_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {"backup_provider_type", required_argument, 0, 'b'},
                                           {"app_ids", required_argument, 0, 'a'},
                                           {"backup_interval_seconds", required_argument, 0, 'i'},
                                           {"start_time", required_argument, 0, 's'},
                                           {"backup_history_cnt", required_argument, 0, 'c'},
                                           {0, 0, 0, 0}};

    std::string policy_name;
    std::string backup_provider_type;
    std::vector<int32_t> app_ids;
    int64_t backup_interval_seconds = 0;
    std::string start_time;
    int32_t backup_history_cnt = 0;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:b:a:i:s:c:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        case 'b':
            backup_provider_type = optarg;
            ;
            break;
        case 'a': {
            std::vector<std::string> app_list;
            ::dsn::utils::split_args(optarg, app_list, ',');
            for (const auto &app : app_list) {
                int32_t id = atoi(app.c_str());
                if (id <= 0) {
                    fprintf(stderr, "invalid app_id, %d\n", id);
                    return false;
                } else {
                    app_ids.emplace_back(id);
                }
            }
        } break;
        case 'i':
            backup_interval_seconds = atoi(optarg);
            break;
        case 's':
            start_time = optarg;
            break;
        case 'c':
            backup_history_cnt = atoi(optarg);
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "policy_name should not be empty\n");
        return false;
    }
    if (backup_provider_type.empty()) {
        fprintf(stderr, "backup_provider_type should not be empty\n");
        return false;
    }

    if (backup_interval_seconds <= 0) {
        fprintf(stderr,
                "invalid backup_interval_seconds: %" PRId64 ", should > 0\n",
                backup_interval_seconds);
        return false;
    }

    if (backup_history_cnt <= 0) {
        fprintf(stderr, "invalid backup_history_cnt: %d, should > 0\n", backup_history_cnt);
        return false;
    }

    if (!start_time.empty()) {
        int32_t hour = 0, min = 0;
        if (sscanf(start_time.c_str(), "%d:%d", &hour, &min) != 2 || hour > 24 || hour < 0 ||
            min >= 60 || min < 0 || (hour == 24 && min > 0)) {
            fprintf(stderr,
                    "invalid start time: %s, should like this hour:minute\n",
                    start_time.c_str());
            return false;
        }
    }

    ::dsn::error_code ret = sc->ddl_client->add_backup_policy(policy_name,
                                                              backup_provider_type,
                                                              app_ids,
                                                              backup_interval_seconds,
                                                              backup_history_cnt,
                                                              start_time);
    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "add backup policy failed, err = %s\n", ret.to_string());
    }
    return true;
}

inline bool ls_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    ::dsn::error_code err = sc->ddl_client->ls_backup_policy();
    if (err != ::dsn::ERR_OK) {
        std::cout << "ls backup policy failed" << std::endl;
    } else {
        std::cout << std::endl << "ls backup policy succeed" << std::endl;
    }
    return true;
}

inline bool query_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {"backup_info_cnt", required_argument, 0, 'b'},
                                           {0, 0, 0, 0}};
    std::vector<std::string> policy_names;
    int backup_info_cnt = 3;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:b:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p': {
            std::vector<std::string> names;
            ::dsn::utils::split_args(optarg, names, ',');
            for (const auto &policy_name : names) {
                if (policy_name.empty()) {
                    fprintf(stderr, "invalid, empty policy_name, just ignore\n");
                    continue;
                } else {
                    policy_names.emplace_back(policy_name);
                }
            }
        } break;
        case 'b':
            backup_info_cnt = atoi(optarg);
            if (backup_info_cnt <= 0) {
                fprintf(stderr, "invalid backup_info_cnt %s\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }
    if (policy_names.empty()) {
        fprintf(stderr, "empty policy_name, please assign policy_name you want to query\n");
        return false;
    }
    ::dsn::error_code ret = sc->ddl_client->query_backup_policy(policy_names, backup_info_cnt);
    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "query backup policy failed, err = %s\n", ret.to_string());
    } else {
        std::cout << std::endl << "query backup policy succeed" << std::endl;
    }
    return true;
}

inline bool modify_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {"add_app", required_argument, 0, 'a'},
                                           {"remove_app", required_argument, 0, 'r'},
                                           {"backup_interval_seconds", required_argument, 0, 'i'},
                                           {"backup_history_count", required_argument, 0, 'c'},
                                           {"start_time", required_argument, 0, 's'},
                                           {0, 0, 0, 0}};

    std::string policy_name;
    std::vector<int32_t> add_appids;
    std::vector<int32_t> remove_appids;
    int64_t backup_interval_seconds = 0;
    int32_t backup_history_count = 0;
    std::string start_time;
    std::vector<std::string> app_id_strs;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:a:r:i:c:s:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        case 'a':
            app_id_strs.clear();
            ::dsn::utils::split_args(optarg, app_id_strs, ',');
            for (const auto &s_appid : app_id_strs) {
                int32_t appid = boost::lexical_cast<int32_t>(s_appid);
                if (appid <= 0) {
                    fprintf(stderr, "add invalid app_id(%d) to policy\n", appid);
                    return false;
                } else {
                    add_appids.emplace_back(boost::lexical_cast<int32_t>(s_appid));
                }
            }
            break;
        case 'r':
            app_id_strs.clear();
            ::dsn::utils::split_args(optarg, app_id_strs, ',');
            for (const auto &s_appid : app_id_strs) {
                int32_t appid = boost::lexical_cast<int32_t>(s_appid);
                if (appid <= 0) {
                    fprintf(stderr, "remove invalid app_id(%d) from policy\n", appid);
                    return false;
                } else {
                    remove_appids.emplace_back(boost::lexical_cast<int32_t>(s_appid));
                }
            }
            break;
        case 'i':
            backup_interval_seconds = boost::lexical_cast<int64_t>(optarg);
            if (backup_interval_seconds < 0) {
                fprintf(stderr,
                        "invalid backup_interval_seconds(%" PRId64 ")\n",
                        backup_interval_seconds);
                return false;
            }
            break;
        case 'c':
            backup_history_count = boost::lexical_cast<int32_t>(optarg);
            if (backup_history_count < 0) {
                fprintf(stderr, "invalid backup_history_count(%d)\n", backup_history_count);
                return false;
            }
            break;
        case 's':
            start_time = optarg;
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "empty policy name\n");
        return false;
    }

    if (!start_time.empty()) {
        int32_t hour = 0, min = 0;
        if (sscanf(start_time.c_str(), "%d:%d", &hour, &min) != 2 || hour > 24 || hour < 0 ||
            min >= 60 || min < 0 || (hour == 24 && min > 0)) {
            fprintf(stderr,
                    "invalid start time: %s, should like this hour:minute\n",
                    start_time.c_str());
            return false;
        }
    }

    dsn::error_code ret = sc->ddl_client->update_backup_policy(policy_name,
                                                               add_appids,
                                                               remove_appids,
                                                               backup_interval_seconds,
                                                               backup_history_count,
                                                               start_time);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "modify backup policy failed, with err = %s\n", ret.to_string());
    }
    return true;
}

inline bool disable_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {0, 0, 0, 0}};

    std::string policy_name;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "empty policy name\n");
        return false;
    }

    ::dsn::error_code ret = sc->ddl_client->disable_backup_policy(policy_name);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "disable backup policy failed, with err = %s\n", ret.to_string());
    }
    return true;
}

inline bool enable_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {0, 0, 0, 0}};
    std::string policy_name;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "empty policy name\n");
        return false;
    }

    ::dsn::error_code ret = sc->ddl_client->enable_backup_policy(policy_name);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "enable backup policy failed, with err = %s\n", ret.to_string());
    }
    return true;
}

inline bool exit_shell(command_executor *e, shell_context *sc, arguments args)
{
    dsn_exit(0);
    return true;
}

inline bool get_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    if (args.argc != 1) {
        return false;
    }

    std::map<std::string, std::string> envs;
    ::dsn::error_code ret = sc->ddl_client->get_app_envs(sc->current_app_name, envs);
    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "get app env failed with err = %s\n", ret.to_string());
        return true;
    }

    std::cout << "get app envs succeed, count = " << envs.size() << std::endl;
    if (!envs.empty()) {
        std::cout << "=================================" << std::endl;
        for (auto &kv : envs)
            std::cout << kv.first << " = " << kv.second << std::endl;
        std::cout << "=================================" << std::endl;
    }

    return true;
}

inline bool set_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    if (args.argc < 3) {
        return false;
    }

    if (((args.argc - 1) & 0x01) == 1) {
        // key & value count must equal 2*n(n >= 1)
        fprintf(stderr, "need speficy the value for key = %s\n", args.argv[args.argc - 1]);
        return true;
    }
    std::vector<std::string> keys;
    std::vector<std::string> values;
    int idx = 1;
    while (idx < args.argc) {
        keys.emplace_back(args.argv[idx++]);
        values.emplace_back(args.argv[idx++]);
    }

    ::dsn::error_code ret = sc->ddl_client->set_app_envs(sc->current_app_name, keys, values);

    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "set app env failed with err = %s\n", ret.to_string());
    }
    return true;
}

inline bool del_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    if (args.argc <= 1) {
        return false;
    }

    std::vector<std::string> keys;
    for (int idx = 1; idx < args.argc; idx++) {
        keys.emplace_back(args.argv[idx]);
    }

    ::dsn::error_code ret = sc->ddl_client->del_app_envs(sc->current_app_name, keys);

    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "del app env failed with err = %s\n", ret.to_string());
    }
    return true;
}

inline bool clear_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    static struct option long_options[] = {
        {"all", no_argument, 0, 'a'}, {"prefix", required_argument, 0, 'p'}, {0, 0, 0, 0}};

    bool clear_all = false;
    std::string prefix;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "ap:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            clear_all = true;
            break;
        case 'p':
            prefix = optarg;
            break;
        default:
            return false;
        }
    }
    if (!clear_all && prefix.empty()) {
        fprintf(stderr, "must specify one of --all and --prefix options\n");
        return false;
    }
    ::dsn::error_code ret = sc->ddl_client->clear_app_envs(sc->current_app_name, clear_all, prefix);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "clear app envs failed with err = %s\n", ret.to_string());
    }
    return true;
}
