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
#include <chrono>
#include <iosfwd>
#include <memory>
#include <unordered_set>
#include <utility>

// Disable class-memaccess warning to facilitate compilation with gcc>7
// https://github.com/Tencent/rapidjson/issues/1700
#pragma GCC diagnostic push
#if defined(__GNUC__) && __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
#include <rapidjson/document.h>

#pragma GCC diagnostic pop

#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "dsn.layer2_types.h"
#include "fmt/core.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service.h"
#include "meta/server_state.h"
#include "meta_admin_types.h"
#include "ranger/ranger_resource_policy.h"
#include "ranger/ranger_resource_policy_manager.h"
#include "ranger_resource_policy_manager.h"
#include "rapidjson/allocators.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_code.h"
#include "utils/blob.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/strings.h"

DSN_DEFINE_uint32(security,
                  update_ranger_policy_interval_sec,
                  5,
                  "The interval seconds of meta "
                  "server to pull the latest "
                  "access control policy from "
                  "Ranger service.");
DSN_DEFINE_string(ranger, ranger_service_url, "", "Apache Ranger service url.");
DSN_DEFINE_string(ranger,
                  ranger_service_name,
                  "",
                  "The name of the policies defined in the Ranger service.");
DSN_DEFINE_string(ranger,
                  legacy_table_database_mapping_policy_name,
                  "__default__",
                  "The name of the Ranger database policy matched by the legacy table(The table "
                  "name does not follow the naming rules of {database_name}.{table_name})");

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

// Pull policies in JSON format from Ranger service.
dsn::error_code pull_policies_from_ranger_service(std::string *ranger_policies)
{
    std::string cmd =
        fmt::format("curl {}/{}", FLAGS_ranger_service_url, FLAGS_ranger_service_name);
    std::stringstream resp;
    if (dsn::utils::pipe_execute(cmd.c_str(), resp) != 0) {
        return dsn::ERR_SYNC_RANGER_POLICIES_FAILED;
    }

    *ranger_policies = resp.str();
    return dsn::ERR_OK;
}
} // anonymous namespace

const std::chrono::milliseconds kLoadRangerPolicyRetryDelayMs(10000);

ranger_resource_policy_manager::ranger_resource_policy_manager(
    dsn::replication::meta_service *meta_svc)
    : _meta_svc(meta_svc), _local_policy_version(-1)
{
    // GLOBAL - kMetadata
    register_rpc_access_type(access_type::kMetadata,
                             {"RPC_CM_LIST_NODES", "RPC_CM_CLUSTER_INFO", "RPC_QUERY_DISK_INFO"},
                             _ac_type_of_global_rpcs);
    // GLOBAL - kControl
    register_rpc_access_type(access_type::kControl,
                             {"RPC_HTTP_SERVICE",
                              "RPC_CM_CONTROL_META",
                              "RPC_CM_START_RECOVERY",
                              "RPC_REPLICA_DISK_MIGRATE",
                              "RPC_ADD_NEW_DISK",
                              "RPC_DETECT_HOTKEY",
                              "RPC_CLI_CLI_CALL_ACK"},
                             _ac_type_of_global_rpcs);
    // DATABASE - kList
    register_rpc_access_type(access_type::kList, {"RPC_CM_LIST_APPS"}, _ac_type_of_database_rpcs);
    // DATABASE - kCreate
    register_rpc_access_type(
        access_type::kCreate, {"RPC_CM_CREATE_APP"}, _ac_type_of_database_rpcs);
    // DATABASE - kDrop
    register_rpc_access_type(
        access_type::kDrop, {"RPC_CM_DROP_APP", "RPC_CM_RECALL_APP"}, _ac_type_of_database_rpcs);
    // DATABASE - kMetadata
    register_rpc_access_type(access_type::kMetadata,
                             {"RPC_CM_QUERY_BACKUP_STATUS",
                              "RPC_CM_QUERY_RESTORE_STATUS",
                              "RPC_CM_QUERY_DUPLICATION",
                              "RPC_CM_QUERY_PARTITION_SPLIT",
                              "RPC_CM_QUERY_BULK_LOAD_STATUS",
                              "RPC_CM_QUERY_MANUAL_COMPACT_STATUS",
                              "RPC_CM_GET_MAX_REPLICA_COUNT",
                              "RPC_CM_GET_ATOMIC_IDEMPOTENT"},
                             _ac_type_of_database_rpcs);
    // DATABASE - kControl
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
                              "RPC_CM_SET_ATOMIC_IDEMPOTENT",
                              "RPC_CM_RENAME_APP"},
                             _ac_type_of_database_rpcs);
}

void ranger_resource_policy_manager::start()
{
    CHECK_NOTNULL(_meta_svc, "");
    _ranger_policy_meta_root = dsn::utils::filesystem::concat_path_unix_style(
        _meta_svc->cluster_root(), "ranger_policy_meta_root");
    tasking::enqueue_timer(
        LPC_USE_RANGER_ACCESS_CONTROL,
        &_tracker,
        [this]() { this->update_policies_from_ranger_service(); },
        std::chrono::seconds(FLAGS_update_ranger_policy_interval_sec),
        0,
        std::chrono::milliseconds(1));
}

access_control_result ranger_resource_policy_manager::allowed(
    const int rpc_code, const std::string &user_name, const std::string &database_name) const
{
    do {
        const auto &ac_type = _ac_type_of_global_rpcs.find(rpc_code);
        // It's not a GLOBAL rpc code.
        if (ac_type == _ac_type_of_global_rpcs.end()) {
            break;
        }

        // Check if it is denied by any GLOBAL policy.
        utils::auto_read_lock l(_global_policies_lock);
        return check_ranger_resource_policy_allowed(_global_policies_cache,
                                                    ac_type->second,
                                                    user_name,
                                                    match_database_type::kNotNeed,
                                                    "",
                                                    "");
    } while (false);

    do {
        const auto &ac_type = _ac_type_of_database_rpcs.find(rpc_code);
        // It's not a DATABASE rpc code.
        if (ac_type == _ac_type_of_database_rpcs.end()) {
            break;
        }

        utils::auto_read_lock l(_database_policies_lock);
        return check_ranger_resource_policy_allowed(
            _database_policies_cache,
            ac_type->second,
            user_name,
            match_database_type::kNeed,
            database_name,
            FLAGS_legacy_table_database_mapping_policy_name);
    } while (false);

    // The check that does not match any resource returns false.
    return access_control_result::kDenied;
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

dsn::error_code ranger_resource_policy_manager::update_policies_from_ranger_service()
{
    std::string ranger_policies;
    LOG_AND_RETURN_NOT_OK(
        ERROR, pull_policies_from_ranger_service(&ranger_policies), "Pull Ranger policies failed.");
    LOG_DEBUG("Pull Ranger policies success.");

    auto err_code = load_policies_from_json(ranger_policies);
    if (err_code == dsn::ERR_RANGER_POLICIES_NO_NEED_UPDATE) {
        LOG_DEBUG("Skip to update local policies.");
        // For the newly created table, its app envs must be empty. This needs to be executed
        // periodically to update the table's app envs, regardless of whether the Ranger policy is
        // updated or not.
        LOG_AND_RETURN_NOT_OK(
            ERROR, sync_policies_to_app_envs(), "Sync policies to app envs failed.");
        return dsn::ERR_OK;
    }
    LOG_AND_RETURN_NOT_OK(ERROR, err_code, "Parse Ranger policies failed.");

    start_to_dump_and_sync_policies();

    return dsn::ERR_OK;
}

dsn::error_code ranger_resource_policy_manager::load_policies_from_json(const std::string &data)
{
    // The Ranger policy pulled from Ranger service demo.
    /*
    {
        "serviceName": "PEGASUS1",
        "serviceId": 1069,
        "policyVersion": 60,
        "policyUpdateTime": 1673254471000,
        "policies": [{
            "id": 5334,
            "guid": "c7918f8c-921a-4f3d-b9d7-bce7009ee5f8",
            "isEnabled": true,
            "version": 13,
            "service": "PEGASUS1",
            "name": "all - database",
            "policyType": 0,
            "policyPriority": 0,
            "description": "Policy for all - database",
            "isAuditEnabled": true,
            "resources": {
                "database": {
                    "values": ["PEGASUS1"],
                    "isExcludes": false,
                    "isRecursive": true
                }
            },
            "policyItems": [{
                "accesses": [{
                    "type": "create",
                    "isAllowed": true
                }, {
                    "type": "drop",
                    "isAllowed": true
                }, {
                    "type": "control",
                    "isAllowed": true
                }, {
                    "type": "metadata",
                    "isAllowed": true
                }, {
                    "type": "list",
                    "isAllowed": true
                }],
                "users": ["PEGASUS1"],
                "groups": [],
                "roles": [],
                "conditions": [],
                "delegateAdmin": true
            }],
            "denyPolicyItems": [],
            "allowExceptions": [],
            "denyExceptions": [],
            "dataMaskPolicyItems": [],
            "rowFilterPolicyItems": [],
            "serviceType": "pegasus",
            "options": {},
            "validitySchedules": [],
            "policyLabels": [],
            "zoneName": "",
            "isDenyAllElse": false
        }],
        "auditMode": "audit-default",
        "serviceConfig": {}
    }
    */
    rapidjson::Document doc;
    doc.Parse(data.c_str());

    // Check if it is needed to update policies.
    RETURN_ERR_IF_MISSING_MEMBER(doc, "policyVersion");
    int remote_policy_version = doc["policyVersion"].GetInt();
    if (_local_policy_version == remote_policy_version) {
        LOG_DEBUG("Ranger policy version: {}, no need to update.", _local_policy_version);
        return dsn::ERR_RANGER_POLICIES_NO_NEED_UPDATE;
    }

    if (_local_policy_version > remote_policy_version) {
        LOG_WARNING("Local Ranger policy version ({}) is larger than remote version ({}), please "
                    "check Ranger services ({}).",
                    _local_policy_version,
                    remote_policy_version,
                    FLAGS_ranger_service_name);
        return dsn::ERR_RANGER_POLICIES_NO_NEED_UPDATE;
    }

    _local_policy_version = remote_policy_version;

    // Update policies.
    _all_resource_policies.clear();

    // TODO(wanghao): it's optional
    // Provide a DATABASE default policy for legacy tables.
    // ranger_resource_policy default_database_policy;
    // ranger_resource_policy::create_default_database_policy(default_database_policy);
    // _all_resource_policies[enum_to_string(resource_type::kDatabase)] = {default_database_policy};

    RETURN_ERR_IF_MISSING_MEMBER(doc, "policies");
    const rapidjson::Value &policies = doc["policies"];
    RETURN_ERR_IF_NOT_ARRAY(policies);
    for (const auto &policy : policies.GetArray()) {
        RETURN_ERR_IF_MISSING_MEMBER(policy, "isEnabled");
        // 1. Check if the policy is enabled or not.
        if (!policy["isEnabled"].IsBool() || !policy["isEnabled"].GetBool()) {
            continue;
        }

        // 2. Parse resource type.
        RETURN_ERR_IF_MISSING_MEMBER(policy, "resources");
        std::map<std::string, std::unordered_set<std::string>> values_of_resource_type;
        for (const auto &resource : policy["resources"].GetObject()) {
            RETURN_ERR_IF_MISSING_MEMBER(resource.value, "values");
            RETURN_ERR_IF_NOT_ARRAY((resource.value)["values"]);
            std::unordered_set<std::string> values;
            for (const auto &v : (resource.value)["values"].GetArray()) {
                values.insert(v.GetString());
            }
            values_of_resource_type.emplace(std::make_pair(resource.name.GetString(), values));
        }

        // 3. Construct ACL policy.
        ranger_resource_policy resource_policy;
        CONTINUE_IF_MISSING_MEMBER(policy, "name");
        resource_policy.name = policy["name"].GetString();

        resource_type rt = resource_type::kUnknown;
        do {
            // TODO(wanghao): refactor the following code
            // parse Ranger policies json string into `values_of_resource_type`, distinguish
            // resource types by `values_of_resource_type.size()`
            if (values_of_resource_type.size() == 1) {
                auto iter = values_of_resource_type.find("global");
                if (iter != values_of_resource_type.end()) {
                    rt = resource_type::kGlobal;
                    break;
                }
                iter = values_of_resource_type.find("database");
                if (iter != values_of_resource_type.end()) {
                    resource_policy.database_names = iter->second;
                    rt = resource_type::kDatabase;
                    break;
                }
            } else if (values_of_resource_type.size() == 2) {
                auto iter1 = values_of_resource_type.find("database");
                auto iter2 = values_of_resource_type.find("table");
                if (iter1 != values_of_resource_type.end() &&
                    iter2 != values_of_resource_type.end()) {
                    resource_policy.database_names = iter1->second;
                    resource_policy.table_names = iter2->second;
                    rt = resource_type::kDatabaseTable;
                    break;
                }
            }
            return dsn::ERR_RANGER_PARSE_ACL;
        } while (false);

        parse_policies_from_json(policy["policyItems"], resource_policy.policies.allow_policies);
        parse_policies_from_json(policy["denyPolicyItems"], resource_policy.policies.deny_policies);
        parse_policies_from_json(policy["allowExceptions"],
                                 resource_policy.policies.allow_policies_exclude);
        parse_policies_from_json(policy["denyExceptions"],
                                 resource_policy.policies.deny_policies_exclude);

        // 4. Add the ACL policy.
        auto ret = _all_resource_policies.emplace(enum_to_string(rt),
                                                  resource_policies({resource_policy}));
        if (!ret.second) {
            ret.first->second.emplace_back(resource_policy);
        }
    }

    return dsn::ERR_OK;
}

void ranger_resource_policy_manager::start_to_dump_and_sync_policies()
{
    LOG_DEBUG("Start to create Ranger policy meta root on remote storage.");
    dsn::task_ptr sync_task = dsn::tasking::create_task(
        LPC_USE_RANGER_ACCESS_CONTROL, &_tracker, [this]() { dump_and_sync_policies(); });
    _meta_svc->get_remote_storage()->create_node(
        _ranger_policy_meta_root,
        LPC_USE_RANGER_ACCESS_CONTROL,
        [this, sync_task](dsn::error_code err) {
            if (err == dsn::ERR_OK || err == dsn::ERR_NODE_ALREADY_EXIST) {
                LOG_DEBUG("Create Ranger policy meta root succeed.");
                sync_task->enqueue();
                return;
            }
            CHECK_EQ(err, dsn::ERR_TIMEOUT);
            LOG_ERROR("Create Ranger policy meta root timeout, retry later.");
            dsn::tasking::enqueue(
                LPC_USE_RANGER_ACCESS_CONTROL,
                &_tracker,
                [this]() { start_to_dump_and_sync_policies(); },
                0,
                kLoadRangerPolicyRetryDelayMs);
        });
}

void ranger_resource_policy_manager::dump_and_sync_policies()
{
    LOG_DEBUG("Start to sync Ranger policies to remote storage.");

    dump_policies_to_remote_storage();
    LOG_DEBUG("Dump Ranger policies to remote storage succeed.");

    update_cached_policies();
    LOG_DEBUG("Update using resources policies succeed.");

    if (dsn::ERR_OK != sync_policies_to_app_envs()) {
        LOG_ERROR("Sync policies to app envs failed.");
    }
}

void ranger_resource_policy_manager::dump_policies_to_remote_storage()
{
    dsn::blob value = json::json_forwarder<all_resource_policies>::encode(_all_resource_policies);
    _meta_svc->get_remote_storage()->set_data(
        _ranger_policy_meta_root, value, LPC_USE_RANGER_ACCESS_CONTROL, [this](dsn::error_code e) {
            if (e == dsn::ERR_OK) {
                LOG_DEBUG("Dump Ranger policies to remote storage succeed.");
                return;
            }
            // The return error code is not 'ERR_TIMEOUT', use assert here.
            CHECK_EQ(e, dsn::ERR_TIMEOUT);
            LOG_ERROR("Dump Ranger policies to remote storage timeout, retry later.");
            dsn::tasking::enqueue(
                LPC_USE_RANGER_ACCESS_CONTROL,
                &_tracker,
                [this]() { dump_policies_to_remote_storage(); },
                0,
                kLoadRangerPolicyRetryDelayMs);
        });
}

void ranger_resource_policy_manager::update_cached_policies()
{
    {
        utils::auto_write_lock l(_global_policies_lock);
        _global_policies_cache.swap(_all_resource_policies[enum_to_string(resource_type::kGlobal)]);
        // TODO(wanghao): provide a query method
    }
    {
        utils::auto_write_lock l(_database_policies_lock);
        _database_policies_cache.swap(
            _all_resource_policies[enum_to_string(resource_type::kDatabase)]);
        // TODO(wanghao): provide a query method
    }
}

dsn::error_code ranger_resource_policy_manager::sync_policies_to_app_envs()
{
    const auto &table_policies =
        _all_resource_policies.find(enum_to_string(resource_type::kDatabaseTable));
    if (table_policies == _all_resource_policies.end()) {
        LOG_INFO("DATABASE_TABLE level policy is empty, skip to sync app envs.");
        return dsn::ERR_OK;
    }

    dsn::replication::configuration_list_apps_response list_resp;
    dsn::replication::configuration_list_apps_request list_req;
    list_req.status = dsn::app_status::AS_AVAILABLE;
    _meta_svc->get_server_state()->list_apps(list_req, list_resp);
    LOG_AND_RETURN_NOT_OK(ERROR, list_resp.err, "list_apps failed.");
    for (const auto &app : list_resp.infos) {
        std::string database_name = get_database_name_from_app_name(app.app_name);
        // Use 'legacy_table_database_mapping_policy_name' for table name of invalid Ranger rules to
        // match datdabase resources.
        if (database_name.empty()) {
            database_name = FLAGS_legacy_table_database_mapping_policy_name;
        }
        std::string table_name = get_table_name_from_app_name(app.app_name);

        auto req = std::make_unique<dsn::replication::configuration_update_app_env_request>();
        req->__set_app_name(app.app_name);
        req->__set_keys({dsn::replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES});
        std::vector<matched_database_table_policy> matched_database_table_policies;
        for (const auto &policy : table_policies->second) {
            // If this table does not match any database, this policy will be skipped and will not
            // be written into app_envs.
            if (policy.database_names.count(database_name) == 0 &&
                policy.database_names.count("*") == 0) {
                continue;
            }
            // If this table does not match any database table, this policy will be skipped and will
            // not be written into app_envs.
            if (policy.table_names.count(table_name) == 0 && policy.table_names.count("*") == 0) {
                continue;
            }
            // This table matches a policy.
            matched_database_table_policy database_table_policy(
                {database_name, table_name, policy.policies});
            // This table matches the policy whose database is "*".
            if (policy.database_names.count(database_name) == 0) {
                CHECK(policy.database_names.count("*") != 0,
                      "the list of database_name must contain *");
                database_table_policy.matched_database_name = "*";
            }
            // This table matches the policy whose database table is "*".
            if (policy.table_names.count(table_name) == 0) {
                CHECK(policy.table_names.count("*") != 0, "the list of table_name must contain *");
                database_table_policy.matched_table_name = "*";
            }
            matched_database_table_policies.emplace_back(database_table_policy);
        }
        if (matched_database_table_policies.empty()) {
            // There is no matched policy, clear app Ranger policy
            req->__set_op(dsn::replication::app_env_operation::type::APP_ENV_OP_DEL);

            dsn::replication::update_app_env_rpc rpc(std::move(req), LPC_USE_RANGER_ACCESS_CONTROL);
            _meta_svc->get_server_state()->del_app_envs(rpc);
            _meta_svc->get_server_state()->wait_all_task();
            LOG_AND_RETURN_NOT_OK(ERROR, rpc.response().err, "del_app_envs failed.");
        } else {
            req->__set_op(dsn::replication::app_env_operation::type::APP_ENV_OP_SET);
            req->__set_values(
                {json::json_forwarder<std::vector<matched_database_table_policy>>::encode(
                     matched_database_table_policies)
                     .to_string()});

            dsn::replication::update_app_env_rpc rpc(std::move(req), LPC_USE_RANGER_ACCESS_CONTROL);
            _meta_svc->get_server_state()->set_app_envs(rpc);
            _meta_svc->get_server_state()->wait_all_task();
            LOG_AND_RETURN_NOT_OK(ERROR, rpc.response().err, "set_app_envs failed.");
        }
    }

    LOG_DEBUG("Sync policies to app envs succeeded.");
    return dsn::ERR_OK;
}

std::string get_database_name_from_app_name(const std::string &app_name)
{
    std::string prefix = utils::find_string_prefix(app_name, '.');
    if (prefix.empty() || prefix == app_name) {
        return std::string();
    }

    return prefix;
}

std::string get_table_name_from_app_name(const std::string &app_name)
{
    std::string database_name = get_database_name_from_app_name(app_name);
    return database_name.empty() ? app_name : app_name.substr(database_name.size() + 1);
}
} // namespace ranger
} // namespace dsn
