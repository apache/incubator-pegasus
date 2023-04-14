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

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "access_controller.h"

namespace dsn {
class message_ex;

namespace ranger {
class ranger_resource_policy_manager;
}

namespace security {

class meta_access_controller : public access_controller
{
public:
    meta_access_controller(
        const std::shared_ptr<ranger::ranger_resource_policy_manager> &policy_manager);

    bool allowed(message_ex *msg, const std::string &app_name = "") const override;

private:
    void register_allowed_rpc_code_list(const std::vector<std::string> &rpc_list);

    std::unordered_set<int> _allowed_rpc_code_list;

    std::shared_ptr<ranger::ranger_resource_policy_manager> _ranger_resource_policy_manager;
};
} // namespace security
} // namespace dsn
