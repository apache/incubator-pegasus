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

#include "ranger_resource_policy.h"

namespace dsn {
namespace ranger {

/*extern*/ access_type operator|(access_type lhs, access_type rhs)
{
    using act = std::underlying_type<access_type>::type;
    return access_type(static_cast<act>(lhs) | static_cast<act>(rhs));
}

/*extern*/ access_type operator&(access_type lhs, access_type rhs)
{
    using act = std::underlying_type<access_type>::type;
    return access_type(static_cast<act>(lhs) & static_cast<act>(rhs));
}

bool policy_item::match(const access_type &ac_type, const std::string &user_name) const
{
    return static_cast<bool>(access_types & ac_type) && users.count(user_name) != 0;
}

bool acl_policies::allowed(const access_type &ac_type, const std::string &user_name) const
{
    // 1. Check if it is not allowed.
    for (const auto &deny_policy : deny_policies) {
        // 1.1. In 'deny_policies'.
        if (!deny_policy.match(ac_type, user_name)) {
            continue;
        }
        bool in_deny_policies_exclude = false;
        for (const auto &deny_policy_exclude : deny_policies_exclude) {
            if (deny_policy_exclude.match(ac_type, user_name)) {
                in_deny_policies_exclude = true;
                break;
            }
        }
        // 1.2. Not in any 'deny_policies_exclude', it's not allowed.
        if (!in_deny_policies_exclude) {
            return false;
        }
    }

    // 2. Check if it is allowed.
    for (const auto &allow_policy : allow_policies) {
        // 2.1. In 'allow_policies'.
        if (!allow_policy.match(ac_type, user_name)) {
            continue;
        }
        for (const auto &allow_policy_exclude : allow_policies_exclude) {
            // 2.2. In some 'allow_policies_exclude', it's not allowed.
            if (allow_policy_exclude.match(ac_type, user_name)) {
                return false;
            }
        }
        // 2.3. Not in any 'allow_policies_exclude', it's allowed.
        return true;
    }

    // 3. Otherwise, it's not allowed.
    return false;
}

} // namespace ranger
} // namespace dsn
