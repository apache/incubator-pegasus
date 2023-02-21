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

#include <map>
#include <string>
#include <set>
#include <vector>

#include <rapidjson/document.h>

#include "common/json_helper.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace ranger {

enum access_type
{
    READ = 0,
    WRITE,
    CREATE,
    DROP,
    LIST,
    METADATA,
    CONTROL,
    ALL,
    INVALID,
};

ENUM_BEGIN(access_type, INVALID)
ENUM_REG(READ)
ENUM_REG(WRITE)
ENUM_REG(CREATE)
ENUM_REG(DROP)
ENUM_REG(LIST)
ENUM_REG(METADATA)
ENUM_REG(CONTROL)
ENUM_REG(ALL)
ENUM_END(access_type)

ENUM_TYPE_SERIALIZATION(access_type, INVALID)

struct policy_item
{
    std::set<access_type> access_types;
    std::set<std::string> users;
    std::set<std::string> groups; // not use
    std::set<std::string> roles;  // not use

    DEFINE_JSON_SERIALIZATION(access_types, users, groups, roles);

    bool match(const access_type &ac_type, const std::string &user_name) const;
};

struct acl_policies
{
    // policy priority: deny_policies_exclude > deny_policies > allow_policies_exclude >
    // allow_policies
    std::vector<policy_item> allow_policies;
    std::vector<policy_item> allow_policies_exclude;
    std::vector<policy_item> deny_policies;
    std::vector<policy_item> deny_policies_exclude;

    DEFINE_JSON_SERIALIZATION(allow_policies,
                              allow_policies_exclude,
                              deny_policies,
                              deny_policies_exclude);

    // Check whether the user is allowed to access the resource.
    bool allowed(const access_type &ac_type, const std::string &user_name) const;
};

struct ranger_resource_policy
{
public:
    ranger_resource_policy() = default;
    ~ranger_resource_policy() = default;

    // Create the default policy to adapt legacy tables.
    static void create_default_database_policy(ranger_resource_policy &acl);

public:
    std::string name;
    std::set<std::string> global_names; // TODO(yingchun): not in use
    std::set<std::string> database_names;
    std::set<std::string> table_names;
    acl_policies policies;

    DEFINE_JSON_SERIALIZATION(name, global_names, database_names, table_names, policies)
};

} // namespace security
} // namespace dsn
