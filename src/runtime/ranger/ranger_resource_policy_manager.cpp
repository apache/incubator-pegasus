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

#include <ctype.h>
#include <algorithm>
#include <unordered_set>
#include <utility>

#include "meta/meta_options.h"
#include "meta/meta_service.h"
#include "ranger_resource_policy_manager.h"
#include "runtime/ranger/ranger_resource_policy.h"
#include "runtime/task/task_code.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace ranger {

#define RETURN_ERR_IF_MISSING_MEMBER(obj, member)                                                  \
    do {                                                                                           \
        if (!obj.IsObject() || !obj.HasMember(member)) {                                           \
            return dsn::ERR_RANGER_PARSE_ACL;                                                      \
        }                                                                                          \
    } while (0)

#define CONTINUE_IF_MISSING_MEMBER(obj, member)                                                    \
    do {                                                                                           \
        if (!obj.IsObject() || !obj.HasMember(member)) {                                           \
            continue;                                                                              \
        }                                                                                          \
    } while (0)

#define RETURN_ERR_IF_NOT_ARRAY(obj)                                                               \
    do {                                                                                           \
        if (!obj.IsArray() || obj.Empty()) {                                                       \
            return dsn::ERR_RANGER_PARSE_ACL;                                                      \
        }                                                                                          \
    } while (0)

#define RETURN_VOID_IF_NOT_ARRAY(obj)                                                              \
    do {                                                                                           \
        if (!obj.IsArray() || obj.Empty()) {                                                       \
            return;                                                                                \
        }                                                                                          \
    } while (0)

namespace {
// Register access types of 'rpc_codes' as 'ac_type' to 'ac_type_of_rpc'.
// TODO(wanghao): A better way is to define the ac_type when defining rpc, and traverse all RPCs to
// register to avoid omission or duplication.
void register_rpc_access_type(access_type ac_type,
                              const std::vector<std::string> &rpc_codes,
                              access_type_of_rpc_code &ac_type_of_rpc)
{
    for (const auto &rpc_code : rpc_codes) {
        auto code = task_code::try_get(rpc_code, TASK_CODE_INVALID);
        CHECK_NE(code, TASK_CODE_INVALID);
        ac_type_of_rpc.emplace(code, ac_type);
    }
}

// Used to map access_type matched resources policies json string.
const std::map<std::string, access_type> kAccessTypeMaping({{"READ", access_type::kRead},
                                                            {"WRITE", access_type::kWrite},
                                                            {"CREATE", access_type::kCreate},
                                                            {"DROP", access_type::kDrop},
                                                            {"LIST", access_type::kList},
                                                            {"METADATA", access_type::kMetadata},
                                                            {"CONTROL", access_type::kControl}});
} // anonymous namespace

ranger_resource_policy_manager::ranger_resource_policy_manager(
    dsn::replication::meta_service *meta_svc)
    : _meta_svc(meta_svc) //, _local_policy_version(0)
{
    _ranger_policy_meta_root = dsn::replication::meta_options::concat_path_unix_style(
        _meta_svc->cluster_root(), "ranger_policy_meta_root");

    // GLOBAL - KMetadata
    register_rpc_access_type(
        access_type::kMetadata,
        {"RPC_CM_LIST_NODES", "RPC_CM_CLUSTER_INFO", "RPC_CM_LIST_APPS", "RPC_QUERY_DISK_INFO"},
        _ac_type_of_global_rpcs);
    // GLOBAL - KControl
    register_rpc_access_type(access_type::kControl,
                             {"RPC_HTTP_SERVICE",
                              "RPC_CM_CONTROL_META",
                              "RPC_CM_START_RECOVERY",
                              "RPC_REPLICA_DISK_MIGRATE",
                              "RPC_ADD_NEW_DISK",
                              "RPC_DETECT_HOTKEY",
                              "RPC_CLI_CLI_CALL_ACK"},
                             _ac_type_of_global_rpcs);
    // DATABASE - KList
    register_rpc_access_type(access_type::kList, {"RPC_CM_LIST_APPS"}, _ac_type_of_database_rpcs);
    // DATABASE - KCreate
    register_rpc_access_type(
        access_type::kCreate, {"RPC_CM_CREATE_APP"}, _ac_type_of_database_rpcs);
    // DATABASE - KDrop
    register_rpc_access_type(
        access_type::kDrop, {"RPC_CM_DROP_APP", "RPC_CM_RECALL_APP"}, _ac_type_of_database_rpcs);
    // DATABASE - KMetadata
    register_rpc_access_type(access_type::kMetadata,
                             {"RPC_CM_QUERY_BACKUP_STATUS",
                              "RPC_CM_QUERY_RESTORE_STATUS",
                              "RPC_CM_QUERY_DUPLICATION",
                              "RPC_CM_QUERY_PARTITION_SPLIT",
                              "RPC_CM_QUERY_BULK_LOAD_STATUS",
                              "RPC_CM_QUERY_MANUAL_COMPACT_STATUS",
                              "RPC_CM_GET_MAX_REPLICA_COUNT"},
                             _ac_type_of_database_rpcs);
    // DATABASE - KControl
    register_rpc_access_type(access_type::kControl,
                             {"RPC_CM_START_BACKUP_APP",
                              "RPC_CM_START_RESTORE",
                              "RPC_CM_PROPOSE_BALANCER",
                              "RPC_CM_ADD_DUPLICATION",
                              "RPC_CM_MODIFY_DUPLICATION",
                              "RPC_CM_UPDATE_APP_ENV",
                              "RPC_CM_DDD_DIAGNOSE",
                              "RPC_CM_START_PARTITION_SPLIT",
                              "RPC_CM_CONTROL_PARTITION_SPLIT",
                              "RPC_CM_START_BULK_LOAD",
                              "RPC_CM_CONTROL_BULK_LOAD",
                              "RPC_CM_CLEAR_BULK_LOAD",
                              "RPC_CM_START_MANUAL_COMPACT",
                              "RPC_CM_SET_MAX_REPLICA_COUNT",
                              "RPC_CM_RENAME_APP"},
                             _ac_type_of_database_rpcs);
}

void ranger_resource_policy_manager::parse_policies_from_json(const rapidjson::Value &data,
                                                              std::vector<policy_item> &policies)
{
    CHECK(policies.empty(), "Ranger policy list should not be empty.");
    RETURN_VOID_IF_NOT_ARRAY(data);
    for (const auto &item : data.GetArray()) {
        CONTINUE_IF_MISSING_MEMBER(item, "accesses");
        policy_item pi;
        for (const auto &access : item["accesses"].GetArray()) {
            CONTINUE_IF_MISSING_MEMBER(access, "isAllowed");
            CONTINUE_IF_MISSING_MEMBER(access, "type");
            if (access["isAllowed"].GetBool()) {
                std::string type = access["type"].GetString();
                std::transform(type.begin(), type.end(), type.begin(), toupper);
                const auto &at = kAccessTypeMaping.find(type);
                // ignore invalid access_type
                if (kAccessTypeMaping.end() != at) {
                    pi.access_types |= at->second;
                }
            }
        }
        CONTINUE_IF_MISSING_MEMBER(item, "users");
        for (const auto &user : item["users"].GetArray()) {
            pi.users.emplace(user.GetString());
        }
        policies.emplace_back(pi);
    }
}
} // namespace ranger
} // namespace dsn
