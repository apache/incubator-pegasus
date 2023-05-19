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

#include "runtime/ranger/access_type.h"

namespace dsn {
namespace ranger {

bool policy_item::match(const access_type &ac_type, const std::string &user_name) const
{
    return static_cast<bool>(access_types & ac_type) && users.count(user_name) != 0;
}

policy_check_status acl_policies::policy_check(const access_type &ac_type,
                                               const std::string &user_name,
                                               policy_check_type check_type) const
{
    std::vector<policy_item> *policies = nullptr;
    std::vector<policy_item> *policies_exclude = nullptr;
    if (check_type == policy_check_type.kAllow) {
        policies = allow_policy;
        policies_exclude = allow_policies_exclude;
    } else if (check_type == policy_check_type.kDeny) {
        policies = deny_policy;
        policies_exclude = deny_policies_exclude;
    } else {
        CHECK(false, "Unsupported policy check type: {}", check_type);
    }
    for (const auto &policy : *policies) {
        // 1.1. Not match a 'allow_policies/deny_policies'.
        if (!policy.match(ac_type, user_name)) {
            continue;
        }
        // 1.2. A policies has been matched.
        bool in_policies_exclude = false;
        // 1.3. In 'allow_policies_exclude/deny_policies_exclude'.
        for (const auto &policy_exclude : policies_exclude) {
            if (policy_exclude.match(ac_type, user_name)) {
                in_policies_exclude = true;
                break;
            }
        }
        if (in_policies_exclude) {
            // 1.5. In any 'policies_exclude'.
            return policy_check_status.kPending;
        } else {
            // 1.6. Not in any 'policies_exclude'.
            if (check_type == policy_check_type.kAllow) {
                return policy_check_status.kAllowed;
            } else {
                return policy_check_status.kDenied;
            }
        }
    }
    // 1.7. not match any policy.
    return policy_check_status.kNotMatched;
}

} // namespace ranger
} // namespace dsn
