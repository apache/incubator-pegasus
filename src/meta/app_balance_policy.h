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

#include <gtest/gtest_prod.h>
#include <memory>
#include <unordered_map>
#include <vector>

#include "load_balance_policy.h"
#include "meta/meta_data.h"
#include "utils/command_manager.h"

namespace dsn {
class gpid;
class rpc_address;

namespace replication {
class meta_service;

class app_balance_policy : public load_balance_policy
{
public:
    app_balance_policy(meta_service *svc);
    ~app_balance_policy() = default;

    void balance(bool checker, const meta_view *global_view, migration_list *list) override;

private:
    bool need_balance_secondaries(bool balance_checker);
    bool copy_secondary(const std::shared_ptr<app_state> &app, bool place_holder);

    std::vector<std::unique_ptr<command_deregister>> _cmds;

    // options
    bool _balancer_in_turn;
    bool _only_primary_balancer;
    bool _only_move_primary;
};

class copy_secondary_operation : public copy_replica_operation
{
public:
    copy_secondary_operation(const std::shared_ptr<app_state> app,
                             const app_mapper &apps,
                             node_mapper &nodes,
                             const std::vector<dsn::rpc_address> &address_vec,
                             const std::unordered_map<dsn::rpc_address, int> &address_id,
                             int replicas_low);
    ~copy_secondary_operation() = default;

private:
    bool can_continue();
    int get_partition_count(const node_state &ns) const;
    bool can_select(gpid pid, migration_list *result);
    balance_type get_balance_type();
    bool only_copy_primary() { return false; }

    int _replicas_low;

    FRIEND_TEST(copy_secondary_operation, misc);
};
} // namespace replication
} // namespace dsn
