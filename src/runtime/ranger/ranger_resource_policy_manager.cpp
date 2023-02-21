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

#include <memory>
#include <string>
#include <utility>

#include "common/replication.codes.h"
#include "common/replica_envs.h"
#include "meta/meta_options.h"
#include "ranger_resource_policy_manager.h"
#include "runtime/task/async_calls.h"
#include "utils/api_utilities.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"

namespace dsn {
namespace ranger {

DSN_DEFINE_uint32(security,
                  update_ranger_policy_interval_sec,
                  5,
                  "The interval seconds of meta "
                  "server to pull the latest "
                  "access control policy from "
                  "Ranger service.");
DSN_DEFINE_string(ranger, ranger_service_url, "", "Apache Ranger service url.");
DSN_DEFINE_string(ranger, ranger_service_name, "", "use policy name.");
// TODO(yingchun): not in use now!
DSN_DEFINE_string(ranger,
                  ranger_legacy_table_database_mapping_rule,
                  "default",
                  "the policy used by legacy tables after the ACL is enabled.");
DSN_DEFINE_bool(ranger,
                mandatory_enable_acl,
                "false",
                "if true, the original policies will be cleared when pull policies failed.");

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
        if (!obj.IsArray() || obj.Size() == 0) {                                                   \
            return dsn::ERR_RANGER_PARSE_ACL;                                                      \
        }                                                                                          \
    } while (0)

#define RETURN_VOID_IF_NOT_ARRAY(obj)                                                              \
    do {                                                                                           \
        if (!obj.IsArray() || obj.Size() == 0) {                                                   \
            return;                                                                                \
        }                                                                                          \
    } while (0)

namespace {
// Register the matching between rpc_code and access contol type in resources.
void register_rpc_access_type(access_type type,
                              const std::vector<std::string> &rpc_codes,
                              access_type_of_rpc_code &ac_type_of_rpc)
{
    for (const auto &rpc_code : rpc_codes) {
        auto code = task_code::try_get(rpc_code, TASK_CODE_INVALID);
        CHECK_NE(code, TASK_CODE_INVALID);
        ac_type_of_rpc.emplace(code, type);
    }
}

// Used to map access_type matched resources policies json string.
std::map<std::string, access_type> access_type_maping({{"READ", READ},
                                                       {"WRITE", WRITE},
                                                       {"CREATE", CREATE},
                                                       {"DROP", DROP},
                                                       {"LIST", LIST},
                                                       {"METADATA", METADATA},
                                                       {"CONTROL", CONTROL},
                                                       {"ALL", ALL}});

// Parse Ranger ACL policies in JSON format 'data' into 'policies'.
void parse_policies_from_json(const rapidjson::Value &data, std::vector<policy_item> &policies)
{
    CHECK(policies.empty(), "Ranger policy list should not be empty.");
    RETURN_VOID_IF_NOT_ARRAY(data);
    for (auto &item : data.GetArray()) {
        policy_item pi;
        CONTINUE_IF_MISSING_MEMBER(item, "accesses");
        for (const auto &access : item["accesses"].GetArray()) {
            if (access["isAllowed"].GetBool()) {
                std::string type = access["type"].GetString();
                std::transform(type.begin(), type.end(), type.begin(), toupper);
                pi.access_types.emplace(access_type_maping[type]);
            }
        }
        CONTINUE_IF_MISSING_MEMBER(item, "users");
        for (const auto &user : item["users"].GetArray()) {
            pi.users.emplace(user.GetString());
        }
        // TODO(yingchun): "groups" and "roles" are not used currently.
        // CONTINUE_IF_MISSING_MEMBER(item, "groups");
        // for (const auto &group : item["groups"].GetArray()) {
        //     pi.groups.insert(group.GetString());
        // }
        // CONTINUE_IF_MISSING_MEMBER(item, "roles");
        // for (const auto &role : item["roles"].GetArray()) {
        //     pi.roles.insert(role.GetString());
        // }
        policies.emplace_back(pi);
    }
}
} // anonymous namespace

std::chrono::milliseconds load_ranger_policy_retry_delay_ms(10000);

ranger_resource_policy_manager::ranger_resource_policy_manager(
    dsn::replication::meta_service *meta_svc)
    : _meta_svc(meta_svc), _local_policy_version(0)
{
    _ranger_policy_meta_root = dsn::replication::meta_options::concat_path_unix_style(
        _meta_svc->cluster_root(), "ranger_policy_meta_root");

    // GLOBAL - METADATA
    register_rpc_access_type(
        access_type::METADATA,
        {"RPC_CM_LIST_NODES", "RPC_CM_CLUSTER_INFO", "RPC_CM_LIST_APPS", "RPC_QUERY_DISK_INFO"},
        _ac_type_of_global_rpcs);
    // GLOBAL - CONTROL
    register_rpc_access_type(access_type::CONTROL,
                             {"RPC_HTTP_SERVICE",
                              "RPC_CM_CONTROL_META",
                              "RPC_CM_START_RECOVERY",
                              "RPC_REPLICA_DISK_MIGRATE",
                              "RPC_ADD_NEW_DISK",
                              "RPC_DETECT_HOTKEY",
                              "RPC_CLI_CLI_CALL_ACK"},
                             _ac_type_of_global_rpcs);
    // DATABASE - LIST
    register_rpc_access_type(access_type::LIST, {"RPC_CM_LIST_APPS"}, _ac_type_of_database_rpcs);
    // DATABASE - CREATE
    register_rpc_access_type(access_type::CREATE, {"RPC_CM_CREATE_APP"}, _ac_type_of_database_rpcs);
    // DATABASE - DROP
    register_rpc_access_type(
        access_type::DROP, {"RPC_CM_DROP_APP", "RPC_CM_RECALL_APP"}, _ac_type_of_database_rpcs);
    // DATABASE - METADATA
    register_rpc_access_type(access_type::METADATA,
                             {"RPC_CM_QUERY_BACKUP_STATUS",
                              "RPC_CM_QUERY_RESTORE_STATUS",
                              "RPC_CM_QUERY_DUPLICATION",
                              "RPC_CM_QUERY_PARTITION_SPLIT",
                              "RPC_CM_QUERY_BULK_LOAD_STATUS",
                              "RPC_CM_QUERY_MANUAL_COMPACT_STATUS",
                              "RPC_CM_GET_MAX_REPLICA_COUNT"},
                             _ac_type_of_database_rpcs);
    // DATABASE - CONTROL
    register_rpc_access_type(access_type::CONTROL,
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

void ranger_resource_policy_manager::start()
{
    tasking::enqueue_timer(LPC_CM_GET_RANGER_POLICY,
                           &_tracker,
                           [this]() { this->update_policies_from_ranger_service(); },
                           std::chrono::seconds(FLAGS_update_ranger_policy_interval_sec),
                           0,
                           std::chrono::milliseconds(1));
}

bool ranger_resource_policy_manager::allowed(const int rpc_code,
                                             const std::string &user_name,
                                             const std::string &database_name)
{
    do {
        const auto &ac_type = _ac_type_of_global_rpcs.find(rpc_code);
        // It's not a GLOBAL rpc code.
        if (ac_type == _ac_type_of_global_rpcs.end()) {
            break;
        }

        // Check if it is allowed by any GLOBAL policy.
        utils::auto_read_lock l(_global_policies_lock);
        for (const auto &policy : _global_policies_cache) {
            if (policy.policies.allowed(ac_type->second, user_name)) {
                return true;
            }
        }

        // It's not allowed to access except list_app.
        // list_app rpc code is in both GLOBAL and DATABASE policies, check the DATABASE policies
        // later.
        if (rpc_code != RPC_CM_LIST_APPS.code()) {
            return false;
        }
    } while (false);

    do {
        const auto &ac_type = _ac_type_of_database_rpcs.find(rpc_code);
        // It's not a DATABASE rpc code.
        if (ac_type == _ac_type_of_database_rpcs.end()) {
            break;
        }

        // Check if it is allowed by any DATABASE policy.
        utils::auto_read_lock l(_database_policies_lock);
        for (const auto &policy : _database_policies_cache) {
            if (!policy.policies.allowed(ac_type->second, user_name)) {
                continue;
            }
            // Legacy tables may don't contain database section.
            if (database_name.empty() && policy.database_names.count("*") != 0) {
                return true;
            }
            if (policy.database_names.count(database_name) != 0) {
                return true;
            }
        }
    } while (false);

    // The check that does not match any policy returns false.
    return false;
}

dsn::error_code ranger_resource_policy_manager::update_policies_from_ranger_service()
{
    std::string ranger_policies;
    ERR_LOG_AND_RETURN_NOT_OK(pull_policies_from_ranger_service(&ranger_policies),
                              "Pull Ranger policies failed.");
    LOG_DEBUG("Pull Ranger policies success.");

    auto err_code = load_policies_from_json(ranger_policies);
    if (err_code == dsn::ERR_RANGER_POLICIES_NO_NEED_UPDATE) {
        LOG_DEBUG("Skip to update local policies.");
        // for the newly created table, its app envs must be empty. This needs to be executed
        // periodically to update the table's app envs, regardless of whether the Ranger policy is
        // updated or not.
        CHECK_EQ_MSG(dsn::ERR_OK, sync_policies_to_app_envs(), "Sync policies to app envs failed.");
        LOG_DEBUG("Sync policies to app envs succeeded.");
        return dsn::ERR_OK;
    }
    ERR_LOG_AND_RETURN_NOT_OK(err_code, "Parse Ranger policies failed.");

    start_to_dump_and_sync_policies();

    return dsn::ERR_OK;
}

void ranger_resource_policy_manager::start_to_dump_and_sync_policies()
{
    LOG_DEBUG("Start to create Ranger policy meta root on remote storage.");
    dsn::task_ptr sync_task = dsn::tasking::create_task(
        LPC_CM_GET_RANGER_POLICY, &_tracker, [this]() { dump_and_sync_policies(); });
    _meta_svc->get_remote_storage()->create_node(
        _ranger_policy_meta_root,
        LPC_CM_GET_RANGER_POLICY, // TODO(yingchun): use another LPC code
        [this, sync_task](dsn::error_code err) {
            if (err == dsn::ERR_OK || err == dsn::ERR_NODE_ALREADY_EXIST) {
                LOG_DEBUG("Create Ranger policy meta root succeed.");
                sync_task->enqueue();
                return;
            }
            CHECK_EQ(err, dsn::ERR_TIMEOUT);
            LOG_ERROR("Create Ranger policy meta root timeout, try it later.");
            dsn::tasking::enqueue(LPC_CM_GET_RANGER_POLICY,
                                  &_tracker,
                                  [this]() { start_to_dump_and_sync_policies(); },
                                  0,
                                  load_ranger_policy_retry_delay_ms);
        });
}

void ranger_resource_policy_manager::dump_and_sync_policies()
{
    LOG_DEBUG("Start to sync Ranger policies to remote storage.");

    dump_policies_to_remote_storage();
    LOG_DEBUG("Dump Ranger policies to remote storage succeed.");

    update_cached_policies();
    LOG_DEBUG("Update using resources policies succeed.");

    CHECK_EQ_MSG(dsn::ERR_OK, sync_policies_to_app_envs(), "Sync policies to app envs failed.");
    LOG_DEBUG("Sync policies to app envs succeeded.");
}

void ranger_resource_policy_manager::dump_policies_to_remote_storage()
{
    dsn::blob value = json::json_forwarder<all_resource_policies>::encode(_all_resource_policies);
    _meta_svc->get_remote_storage()->set_data(
        _ranger_policy_meta_root, value, LPC_CM_GET_RANGER_POLICY, [this](dsn::error_code e) {
            if (e == dsn::ERR_OK) {
                LOG_DEBUG("Dump Ranger policies to remote storage succeed.");
                return;
            }
            CHECK_EQ_MSG(e, dsn::ERR_TIMEOUT, "Dump Ranger policies to remote storage failed.");
            LOG_ERROR("Dump Ranger policies to remote storage timeout, retry later.");
            dsn::tasking::enqueue(LPC_CM_GET_RANGER_POLICY,
                                  &_tracker,
                                  [this]() { dump_policies_to_remote_storage(); },
                                  0,
                                  load_ranger_policy_retry_delay_ms);
        });
}

void ranger_resource_policy_manager::update_cached_policies()
{
    {
        utils::auto_write_lock l(_global_policies_lock);
        _global_policies_cache.swap(_all_resource_policies[enum_to_string(resource_type::GLOBAL)]);
        // TODO(yingchun): provide a query method
    }
    {
        utils::auto_write_lock l(_database_policies_lock);
        _database_policies_cache.swap(
            _all_resource_policies[enum_to_string(resource_type::DATABASE)]);
        // TODO(yingchun): provide a query method
    }
}

dsn::error_code ranger_resource_policy_manager::sync_policies_to_app_envs()
{
    const auto &table_policies =
        _all_resource_policies.find(enum_to_string(resource_type::DATABASE_TABLE));
    if (table_policies == _all_resource_policies.end()) {
        LOG_INFO("DATABASE_TABLE level policy is empty, skip to sync app envs.");
        return dsn::ERR_OK;
    }

    dsn::replication::configuration_list_apps_response list_resp;
    dsn::replication::configuration_list_apps_request list_req;
    list_req.status = dsn::app_status::AS_AVAILABLE;
    _meta_svc->get_server_state()->list_apps(list_req, list_resp);
    ERR_LOG_AND_RETURN_NOT_OK(list_resp.err, "list_apps failed.");
    for (const auto &app : list_resp.infos) {
        std::string database_name = get_database_name_from_app_name(app.app_name);
        std::string table_name;
        if (database_name.empty()) {
            database_name = "*";
            table_name = app.app_name;
        } else {
            table_name = app.app_name.substr(database_name.size());
        }

        auto req = dsn::make_unique<dsn::replication::configuration_update_app_env_request>();
        req->__set_app_name(app.app_name);
        req->__set_keys(
            {dsn::replication::replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES});
        bool has_match_policy = false;
        for (const auto &policy : table_policies->second) {
            if (policy.database_names.count(database_name) == 0) {
                continue;
            }

            // if table name does not conform to the naming rules(database_name.table_name),
            // database is defined by "*" in ranger for acl matching
            if (policy.table_names.count("*") != 0 || policy.table_names.count(table_name) != 0) {
                has_match_policy = true;
                req->__set_op(dsn::replication::app_env_operation::type::APP_ENV_OP_SET);
                req->__set_values(
                    {json::json_forwarder<acl_policies>::encode(policy.policies).to_string()});

                dsn::replication::update_app_env_rpc rpc(std::move(req), LPC_CM_GET_RANGER_POLICY);
                _meta_svc->get_server_state()->set_app_envs(rpc);
                ERR_LOG_AND_RETURN_NOT_OK(rpc.response().err, "set_app_envs failed.");
                break;
            }
        }

        // There is no matched policy, clear app Ranger policy
        if (!has_match_policy) {
            req->__set_op(dsn::replication::app_env_operation::type::APP_ENV_OP_DEL);

            dsn::replication::update_app_env_rpc rpc(std::move(req), LPC_CM_GET_RANGER_POLICY);
            _meta_svc->get_server_state()->del_app_envs(rpc);
            ERR_LOG_AND_RETURN_NOT_OK(rpc.response().err, "del_app_envs failed.");
        }
    }

    return dsn::ERR_OK;
}

dsn::error_code ranger_resource_policy_manager::pull_policies_from_ranger_service(
    std::string *ranger_policies) const
{
    std::string cmd =
        fmt::format("curl {}/{}", FLAGS_ranger_service_url, FLAGS_ranger_service_name);
    std::stringstream resp;
    if (dsn::utils::pipe_execute(cmd.c_str(), resp) != 0) {
        // get policies failed from Ranger service.
        if (FLAGS_mandatory_enable_acl) {
            // TODO(wanghao): clear all policy
            LOG_ERROR("get policy failed, local policies will be cleared later.");
        } else {
            LOG_WARNING("get policy failed, will use outdated policy.");
        }
        return dsn::ERR_SYNC_RANGER_POLICIES_FAILED;
    }

    *ranger_policies = resp.str();
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

    if (_local_policy_version == 0) {
        _local_policy_version = remote_policy_version;
    }

    // Update policies.
    _all_resource_policies.clear();

    // TODO(yingchun): it's optional
    // Provide a DATABASE default policy for legacy tables.
    ranger_resource_policy default_database_policy;
    ranger_resource_policy::create_default_database_policy(default_database_policy);
    _all_resource_policies[enum_to_string(DATABASE)] = {default_database_policy};

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
        std::map<std::string, std::set<std::string>> values_of_resource_type;
        for (const auto &resource : policy["resources"].GetObject()) {
            RETURN_ERR_IF_MISSING_MEMBER(resource.value, "values");
            RETURN_ERR_IF_NOT_ARRAY((resource.value)["values"]);
            std::set<std::string> values;
            for (const auto &v : (resource.value)["values"].GetArray()) {
                values.insert(v.GetString());
            }
            values_of_resource_type.emplace(std::make_pair(resource.name.GetString(), values));
        }

        // 3. Construct ACL policy.
        ranger_resource_policy resource_policy;
        CONTINUE_IF_MISSING_MEMBER(policy, "name");
        resource_policy.name = policy["name"].GetString();

        resource_type rt = UNKNOWN;
        do {
            // TODO(yingchun): refactor the following code
            // parse Ranger policies json string into `values_of_resource_type`, distinguish
            // resource types by `values_of_resource_type.size()`
            if (values_of_resource_type.size() == 1) {
                auto iter = values_of_resource_type.find("global");
                if (iter != values_of_resource_type.end()) {
                    resource_policy.global_names = iter->second;
                    rt = resource_type::GLOBAL;
                    break;
                }
                iter = values_of_resource_type.find("database");
                if (iter != values_of_resource_type.end()) {
                    resource_policy.database_names = iter->second;
                    rt = resource_type::DATABASE;
                    break;
                }
            } else if (values_of_resource_type.size() == 2) {
                auto iter1 = values_of_resource_type.find("database");
                auto iter2 = values_of_resource_type.find("table");
                if (iter1 != values_of_resource_type.end() &&
                    iter2 != values_of_resource_type.end()) {
                    resource_policy.database_names = iter1->second;
                    resource_policy.table_names = iter2->second;
                    rt = resource_type::DATABASE_TABLE;
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

std::string get_database_name_from_app_name(const std::string &app_name)
{
    std::string prefix = utils::find_string_prefix(app_name, '.');
    if (prefix.empty() || prefix == app_name) {
        return std::string();
    }

    return prefix;
}
} // namespace ranger
} // namespace dsn
