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

#include "shell/validate_utils.h"

bool validate_cmd(const argh::parser &cmd,
                  const std::set<std::string> &params,
                  const std::set<std::string> &flags)
{
    if (cmd.size() > 1) {
        fmt::print(stderr, "too many params!\n");
        return false;
    }

    for (const auto &param : cmd.params()) {
        if (params.find(param.first) == params.end()) {
            fmt::print(stderr, "unknown param {} = {}\n", param.first, param.second);
            return false;
        }
    }

    for (const auto &flag : cmd.flags()) {
        if (params.find(flag) != params.end()) {
            fmt::print(stderr, "missing value of {}\n", flag);
            return false;
        }

        if (flags.find(flag) == flags.end()) {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    return true;
}

bool validate_ip(shell_context *sc,
                 const std::string &ip_str,
                 /*out*/ dsn::rpc_address &target_address,
                 /*out*/ std::string &err_info)
{
    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        err_info = fmt::format("list nodes failed, error={} \n", error.to_string());
        return false;
    }

    bool not_find_ip = true;
    for (const auto &node : nodes) {
        if (ip_str == node.first.to_std_string()) {
            target_address = node.first;
            not_find_ip = false;
        }
    }
    if (not_find_ip) {
        err_info = fmt::format("invalid ip, error={} \n", ip_str);
        return false;
    }
    return true;
}
