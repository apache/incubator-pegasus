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

#include "meta/meta_service.h"
#include "meta/server_load_balancer.h"

namespace dsn {
namespace replication {

class dummy_balancer : public server_load_balancer
{
public:
    dummy_balancer(meta_service *s) : server_load_balancer(s) {}
    virtual bool balance(meta_view view, migration_list &list) { return false; }
    virtual bool check(meta_view view, migration_list &list) { return false; }
    virtual void report(const migration_list &list, bool balance_checker) {}
    virtual std::string get_balance_operation_count(const std::vector<std::string> &args)
    {
        return std::string("unknown");
    }
    virtual void score(meta_view view, double &primary_stddev, double &total_stddev) {}
};

} // namespace replication
} // namespace dsn
