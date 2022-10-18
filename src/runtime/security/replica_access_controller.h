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

#include "utils/synchronize.h"
#include "access_controller.h"

namespace dsn {
namespace security {
class replica_access_controller : public access_controller
{
public:
    explicit replica_access_controller(const std::string &name);
    bool allowed(message_ex *msg) override;
    void update(const std::string &users) override;

private:
    utils::rw_lock_nr _lock; // [
    std::unordered_set<std::string> _users;
    std::string _env_users;
    // ]
    std::string _name;

    friend class replica_access_controller_test;
};
} // namespace security
} // namespace dsn
