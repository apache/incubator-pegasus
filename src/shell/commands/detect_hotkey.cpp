// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "backup_types.h"
#include "bulk_load_types.h"
#include "consensus_types.h"
#include "duplication_types.h"
#include "meta_admin_types.h"
#include "partition_split_types.h"
#include "replica_admin_types.h"
#include "shell/argh.h"
#include "shell/commands.h"
#include "utils/strings.h"

bool generate_hotkey_request(dsn::replication::detect_hotkey_request &req,
                             const std::string &hotkey_action,
                             const std::string &hotkey_type,
                             int app_id,
                             int partition_index,
                             std::string &err_info)
{
    if (dsn::utils::iequals(hotkey_type, "read")) {
        req.type = dsn::replication::hotkey_type::type::READ;
    } else if (dsn::utils::iequals(hotkey_type, "write")) {
        req.type = dsn::replication::hotkey_type::type::WRITE;
    } else {
        err_info = fmt::format("\"{}\" is an invalid hotkey type (should be 'read' or 'write')\n",
                               hotkey_type);
        return false;
    }

    if (dsn::utils::iequals(hotkey_action, "start")) {
        req.action = dsn::replication::detect_action::START;
    } else if (dsn::utils::iequals(hotkey_action, "stop")) {
        req.action = dsn::replication::detect_action::STOP;
    } else if (dsn::utils::iequals(hotkey_action, "query")) {
        req.action = dsn::replication::detect_action::QUERY;
    } else {
        err_info =
            fmt::format("\"{}\" is an invalid hotkey detect action (should be 'start' or 'stop')\n",
                        hotkey_action);
        return false;
    }
    req.pid = dsn::gpid(app_id, partition_index);
    return true;
}

// TODO: (Tangyanzhao) merge hotspot_partition_calculator::send_detect_hotkey_request
bool detect_hotkey(command_executor *e, shell_context *sc, arguments args)
{
    // detect_hotkey
    // <-a|--app_id str><-p|--partition_index num><-t|--hotkey_type read|write>
    // <-c|--detect_action start|stop|query><-d|--address str>
    const std::set<std::string> params = {"a",
                                          "app_id",
                                          "p",
                                          "partition_index",
                                          "c",
                                          "hotkey_action",
                                          "t",
                                          "hotkey_type",
                                          "d",
                                          "address"};
    const std::set<std::string> flags = {};
    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    if (!validate_cmd(cmd, params, flags)) {
        return false;
    }

    int app_id;
    if (!dsn::buf2int32(cmd({"-a", "--app_id"}).str(), app_id)) {
        fmt::print(stderr, "\"{}\" is an invalid num\n", cmd({"-a", "--app_id"}).str());
        return false;
    }

    int partition_index;
    if (!dsn::buf2int32(cmd({"-p", "--partition_index"}).str(), partition_index)) {
        fmt::print(stderr, "\"{}\" is an invalid num\n", cmd({"-p", "--partition_index"}).str());
        return false;
    }

    dsn::rpc_address target_address;
    std::string err_info;
    std::string ip_str = cmd({"-d", "--address"}).str();
    if (!validate_ip(sc, ip_str, target_address, err_info)) {
        fmt::print(stderr, "{}\n", err_info);
        return false;
    }

    std::string hotkey_action = cmd({"-c", "--hotkey_action"}).str();
    std::string hotkey_type = cmd({"-t", "--hotkey_type"}).str();
    dsn::replication::detect_hotkey_request req;
    if (!generate_hotkey_request(
            req, hotkey_action, hotkey_type, app_id, partition_index, err_info)) {
        fmt::print(stderr, err_info);
        return false;
    }

    detect_hotkey_response resp;
    auto err = sc->ddl_client->detect_hotkey(dsn::rpc_address(target_address), req, resp);
    if (err != dsn::ERR_OK) {
        fmt::print(stderr,
                   "Hotkey detection rpc sending failed, in {}.{}, error_hint:{}\n",
                   app_id,
                   partition_index,
                   err.to_string());
        return true;
    }

    if (resp.err != dsn::ERR_OK) {
        fmt::print(stderr,
                   "Hotkey detection performed failed, in {}.{}, error_hint:{} {}\n",
                   app_id,
                   partition_index,
                   resp.err,
                   resp.err_hint);
        return true;
    }

    switch (req.action) {
    case dsn::replication::detect_action::START:
        fmt::print("Hotkey detection is starting, using 'detect_hotkey -a {} -p {} -t {} -c "
                   "query -d {}' to get the result later\n",
                   app_id,
                   partition_index,
                   hotkey_type,
                   ip_str);
        break;
    case dsn::replication::detect_action::STOP:
        fmt::print("Hotkey detection is stopped now\n");
        break;
    case dsn::replication::detect_action::QUERY:
        fmt::print("Find {} hotkey in {}.{} result:{}\n",
                   hotkey_type,
                   app_id,
                   partition_index,
                   resp.hotkey_result);
        break;
    }

    return true;
}
