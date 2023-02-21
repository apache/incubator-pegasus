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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "ranger_resource_policy.h"
#include "runtime/api_task.h"
#include "runtime/task/task_tracker.h"
#include "utils/error_code.h"

namespace dsn {

namespace replication {
class meta_service;
class server_state;
}

namespace ranger {

enum resource_type
{
    GLOBAL = 0,
    DATABASE,
    DATABASE_TABLE,
    UNKNOWN,
};

ENUM_BEGIN(resource_type, UNKNOWN)
ENUM_REG(GLOBAL)
ENUM_REG(DATABASE)
ENUM_REG(DATABASE_TABLE)
ENUM_END(resource_type)

ENUM_TYPE_SERIALIZATION(resource_type, UNKNOWN)

// Policies corresponding to a resource
using resource_policies = std::vector<ranger_resource_policy>;
// Policies corresponding to all resources
using all_resource_policies = std::map<std::string, resource_policies>;
// Range access type of rpc codes
using access_type_of_rpc_code = std::unordered_map<int, ranger::access_type>;

class ranger_resource_policy_manager
{
public:
    ranger_resource_policy_manager(dsn::replication::meta_service *meta_svc);

    ~ranger_resource_policy_manager() = default;

    // When using Ranger for ACL, periodically pull policies from Ranger service.
    void start();

    // Update policies from Ranger service.
    dsn::error_code update_policies_from_ranger_service();

    // Return true if the 'user_name' is allowed to access 'app_name' via 'rpc_code'.
    bool allowed(const int rpc_code, const std::string &user_name, const std::string &app_name);

private:
    // Create the path to save policies in remote storage, and update using resources policies.
    void start_to_dump_and_sync_policies();

    // Sync policies in use from Ranger service.
    void dump_and_sync_policies();

    // Dump policies to remote storage.
    void dump_policies_to_remote_storage();

    // Update the cached global/database resources policies.
    void update_cached_policies();

    // Sync policies to app_envs(REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES).
    dsn::error_code sync_policies_to_app_envs();

    // Load policies from JSON formated string.
    dsn::error_code load_policies_from_json(const std::string &data);

    // Pull policies in JSON format from Ranger service.
    dsn::error_code pull_policies_from_ranger_service(std::string *ranger_policies) const;

private:
    dsn::task_tracker _tracker;

    // The path where policies to be saved in remote storage.
    std::string _ranger_policy_meta_root;

    replication::meta_service *_meta_svc;

    // The cache of the global resources policies, it's a subset of '_all_resource_policies'.
    utils::rw_lock_nr _global_policies_lock; // [
    resource_policies _global_policies_cache;
    // ]

    // The cache of the database resources policies, it's a subset of '_all_resource_policies'.
    utils::rw_lock_nr _database_policies_lock; // [
    resource_policies _database_policies_cache;
    // ]

    // Save the rpc codes that match the global resources.
    access_type_of_rpc_code _ac_type_of_global_rpcs;

    // Save the rpc codes that match the database resources.
    access_type_of_rpc_code _ac_type_of_database_rpcs;

    // Record the Ranger policy version to determine whether to update.
    int _local_policy_version;

    // All Ranger ACL policies.
    all_resource_policies _all_resource_policies;

    DEFINE_JSON_SERIALIZATION(_local_policy_version, _all_resource_policies);
};

// Try to get the database name of 'app_name'.
// When using Ranger for ACL, the constraint table naming rule is
// "{database_name}.{table_name}", use "." to split database name and table name.
// Return an empty string if 'app_name' is not a valid Ranger rule table name.
std::string get_database_name_from_app_name(const std::string &app_name);
} // namespace ranger
} // namespace dsn
