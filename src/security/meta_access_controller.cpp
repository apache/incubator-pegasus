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

#include <utility>
#include <vector>

#include "gutil/map_util.h"
#include "ranger/ranger_resource_policy.h"
#include "ranger/ranger_resource_policy_manager.h"
#include "rpc/network.h"
#include "rpc/rpc_message.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DEFINE_string(security,
                  meta_acl_rpc_allow_list,
                  "",
                  "allowed list of rpc codes for meta_access_controller");

DSN_DECLARE_bool(enable_acl);
DSN_DECLARE_bool(enable_ranger_acl);

namespace dsn {
namespace security {

meta_access_controller::meta_access_controller(
    std::shared_ptr<ranger::ranger_resource_policy_manager> policy_manager)
    : _ranger_resource_policy_manager(std::move(policy_manager))
{
    // For the legacy ACL:
    // 1. the meta server accepts and processes any RPC requests in the allow list from
    // all users;
    // 2. for the RPC requests not in the allow list, only those issued by the superuser
    // are accepted.
    if (utils::is_empty(FLAGS_meta_acl_rpc_allow_list)) {
        register_allowed_rpc_code_list({"RPC_CM_CLUSTER_INFO",
                                        "RPC_CM_LIST_APPS",
                                        "RPC_CM_DDD_DIAGNOSE",
                                        "RPC_CM_LIST_NODES",
                                        "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX"});
    } else {
        std::vector<std::string> rpc_code_white_list;
        utils::split_args(FLAGS_meta_acl_rpc_allow_list, rpc_code_white_list, ',');
        register_allowed_rpc_code_list(rpc_code_white_list);
    }

    // Once Ranger ACL is enabled, the allow list registered by the legacy ACL will be
    // cleared and replaced by the allow list from the Ranger ACL.
    if (FLAGS_enable_ranger_acl) {
        register_allowed_rpc_code_list({"RPC_BULK_LOAD",
                                        "RPC_CALL_RAW_MESSAGE",
                                        "RPC_CALL_RAW_SESSION_DISCONNECT",
                                        "RPC_CLEAR_COLD_BACKUP",
                                        "RPC_CM_CONFIG_SYNC",
                                        "RPC_CM_DUPLICATION_SYNC",
                                        "RPC_CM_NOTIFY_STOP_SPLIT",
                                        "RPC_CM_QUERY_CHILD_STATE",
                                        "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX",
                                        "RPC_CM_REPORT_RESTORE_STATUS",
                                        "RPC_CM_UPDATE_PARTITION_CONFIGURATION",
                                        "RPC_CONFIG_PROPOSAL",
                                        "RPC_COLD_BACKUP",
                                        "RPC_FD_FAILURE_DETECTOR_PING",
                                        "RPC_GROUP_BULK_LOAD",
                                        "RPC_GROUP_CHECK",
                                        "RPC_LEARN_ADD_LEARNER",
                                        "RPC_LEARN_COMPLETION_NOTIFY",
                                        "RPC_NEGOTIATION",
                                        "RPC_PREPARE",
                                        "RPC_QUERY_APP_INFO",
                                        "RPC_QUERY_LAST_CHECKPOINT_INFO",
                                        "RPC_QUERY_REPLICA_INFO",
                                        "RPC_REMOVE_REPLICA",
                                        "RPC_SPLIT_NOTIFY_CATCH_UP",
                                        "RPC_SPLIT_UPDATE_CHILD_PARTITION_COUNT"});
        _ranger_resource_policy_manager->start();
    }
}

bool meta_access_controller::allowed(message_ex *msg, const std::string &app_name) const
{
    // Once the Ranger ACL is not enabled, the legacy ACL check will pass as long as any one
    // of these three conditions is met:
    // 1. the legacy ACL is disabled, or
    // 2. the client user is a super user, or
    // 3. the RPC code is in the allow list.
    if (!FLAGS_enable_ranger_acl) {
        return !FLAGS_enable_acl || is_super_user(msg->io_session->get_client_username()) ||
               rpc_allowed(msg->rpc_code().code());
    }

    // Once the Ranger ACL is enabled, the ACL check will pass as long as any one of these
    // two conditions is met:
    // 1. the RPC code is in the allow list (usually internal RPCs), or
    // 2. the username and the target database have both been verified through the Ranger
    // policy.
    const auto rpc_code = msg->rpc_code().code();
    if (rpc_allowed(rpc_code)) {
        return true;
    }

    const auto &user_name = msg->io_session->get_client_username();
    const auto &database_name = ranger::get_database_name_from_app_name(app_name);
    LOG_DEBUG("Ranger access controller with user_name = {}, rpc = {}, database_name = {}",
              user_name,
              msg->rpc_code(),
              database_name);
    return _ranger_resource_policy_manager->allowed(rpc_code, user_name, database_name) ==
           ranger::access_control_result::kAllowed;
}

bool meta_access_controller::rpc_allowed(int rpc_code) const
{
    return gutil::ContainsKey(_allowed_rpc_code_list, rpc_code);
}

void meta_access_controller::register_allowed_rpc_code_list(
    const std::vector<std::string> &rpc_list)
{
    _allowed_rpc_code_list.clear();
    for (const auto &rpc_code : rpc_list) {
        auto code = task_code::try_get(rpc_code, TASK_CODE_INVALID);
        CHECK_NE_MSG(code, TASK_CODE_INVALID, "invalid task code(code = {}).", rpc_code);
        _allowed_rpc_code_list.insert(code);
    }
}

} // namespace security
} // namespace dsn
