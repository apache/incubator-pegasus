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

#include "shell/commands.h"
#include "shell/validate_utils.h"
#include <dsn/dist/replication/replication_types.h>

bool generate_hotkey_request(dsn::replication::detect_hotkey_request &req,
                             const std::string &hotkey_action,
                             const std::string &hotkey_type,
                             int app_id,
                             int partition_index,
                             std::string &err_info)
{
    if (std::strcasecmp(hotkey_type, "read")) {
        req.type = dsn::replication::hotkey_type::type::READ;
    } else if (std::strcasecmp(hotkey_type, "write")) {
        req.type = dsn::replication::hotkey_type::type::WRITE;
    } else {
        err_info = fmt::format("\"{}\" is an invalid hotkey type (should be 'read' or 'write')\n",
                               hotkey_type);
        return false;
    }
    if (std::strcasecmp(hotkey_action, "start")) {
        req.action = dsn::replication::detect_action::START;
    } else if (std::strcasecmp(hotkey_action, "stop")) {
        req.action = dsn::replication::detect_action::STOP;
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
    // detect_hotkey [-a|--app_id] [-p|--partition_index][-c|--hotkey_action][-t|--hotkey_type]
    const std::set<std::string> &params = {"a",
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
        fmt::print(
            stderr,
            "\"{}\" is a invalid app_id, you should use `app` to check related information\n",
            cmd({"-a", "--app_id"}).str());
        return false;
    }
    int partition_index;
    if (!dsn::buf2int32(cmd({"-p", "--partition_index"}).str(), partition_index)) {
        fmt::print(stderr,
                   "\"{}\" is a invalid partition index, you should use `app` to check related "
                   "information\n",
                   cmd({"-p", "--partition_index"}).str());
        return false;
    }
    std::string hotkey_action = cmd({"-c", "--hotkey_action"}).str();
    std::string hotkey_type = cmd({"-t", "--hotkey_type"}).str();
    dsn::rpc_address target_address;
    std::string err_info;

    if (!validate_ip(sc, cmd({"-d", "--address"}).str(), target_address, err_info)) {
        fmt::print(stderr, err_info);
        return false;
    }

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
                   "Hotkey detect rpc sending failed, in {}.{}, error_hint:{}\n",
                   app_id,
                   partition_index,
                   err.to_string());
        return false;
    }

    if (resp.err != dsn::ERR_OK) {
        fmt::print(stderr,
                   "Hotkey detect rpc performed failed, in {}.{}, error_hint:{} {}\n",
                   app_id,
                   partition_index,
                   resp.err,
                   resp.err_hint);
        return false;
    }

    fmt::print(stderr, "YES\n");

    return true;
}
