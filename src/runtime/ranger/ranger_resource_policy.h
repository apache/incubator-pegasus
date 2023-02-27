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

#pragma once

#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include <rapidjson/document.h>

#include "common/json_helper.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace ranger {

// ACL type defined in Range service for RPC matching policy
enum class access_type : uint8_t
{
    KRead = 1,
    KWrite = 1 << 1,
    KCreate = 1 << 2,
    KDrop = 1 << 3,
    KList = 1 << 4,
    KMetadata = 1 << 5,
    KControl = 1 << 6
};

extern access_type operator|(access_type lhs, access_type rhs);

extern access_type operator&(access_type lhs, access_type rhs);

// Ranger policy data structure
struct policy_item
{
    access_type access_types;
    std::unordered_set<std::string> users;

    // Check if the 'acl_type' - 'user_name' pair is matched to the policy.
    // Return true if it is matched, otherwise return false.
    // TODO(wanghao): add benchmark test
    bool match(const access_type &ac_type, const std::string &user_name) const;
};

// Data structure of policies with different priorities
struct acl_policies
{
    // policy priority: deny_policies_exclude > deny_policies > allow_policies_exclude >
    // allow_policies
    std::vector<policy_item> allow_policies;
    std::vector<policy_item> allow_policies_exclude;
    std::vector<policy_item> deny_policies;
    std::vector<policy_item> deny_policies_exclude;

    // Check whether the 'user_name' is allowed to access the resource by type of 'ac_type'.
    bool allowed(const access_type &ac_type, const std::string &user_name) const;
};

// A policy data structure definition of ranger resources
struct ranger_resource_policy
{
    std::string name;
    std::unordered_set<std::string> database_names;
    std::unordered_set<std::string> table_names;
    acl_policies policies;
};

} // namespace ranger
} // namespace dsn
