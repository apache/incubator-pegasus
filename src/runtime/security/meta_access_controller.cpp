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

#include "meta_access_controller.h"

#include "runtime/rpc/network.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

namespace dsn {
namespace security {
DSN_DEFINE_string(security,
                  meta_acl_rpc_allow_list,
                  "",
                  "allowed list of rpc codes for meta_access_controller");

meta_access_controller::meta_access_controller()
{
    // MetaServer serves the allow-list RPC from all users. RPCs unincluded are accessible to only
    // superusers.
    if (utils::is_empty(FLAGS_meta_acl_rpc_allow_list)) {
        register_allowed_list("RPC_CM_LIST_APPS");
        register_allowed_list("RPC_CM_LIST_NODES");
        register_allowed_list("RPC_CM_CLUSTER_INFO");
        register_allowed_list("RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX");
    } else {
        std::vector<std::string> rpc_code_white_list;
        utils::split_args(FLAGS_meta_acl_rpc_allow_list, rpc_code_white_list, ',');
        for (const auto &rpc_code : rpc_code_white_list) {
            register_allowed_list(rpc_code);
        }
    }
}

bool meta_access_controller::allowed(message_ex *msg)
{
    if (pre_check(msg->io_session->get_client_username()) ||
        _allowed_rpc_code_list.find(msg->rpc_code().code()) != _allowed_rpc_code_list.end()) {
        return true;
    }
    return false;
}

void meta_access_controller::register_allowed_list(const std::string &rpc_code)
{
    auto code = task_code::try_get(rpc_code, TASK_CODE_INVALID);
    CHECK_NE_MSG(code,
                 TASK_CODE_INVALID,
                 "invalid task code({}) in rpc_code_white_list of security section",
                 rpc_code);

    _allowed_rpc_code_list.insert(code);
}
} // namespace security
} // namespace dsn
