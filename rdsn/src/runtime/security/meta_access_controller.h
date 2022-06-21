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

#include "access_controller.h"

#include <unordered_set>

namespace dsn {
class message_ex;
namespace security {

class meta_access_controller : public access_controller
{
public:
    meta_access_controller();
    bool allowed(message_ex *msg) override;

private:
    void register_allowed_list(const std::string &rpc_code);

    std::unordered_set<int> _allowed_rpc_code_list;
};
} // namespace security
} // namespace dsn
