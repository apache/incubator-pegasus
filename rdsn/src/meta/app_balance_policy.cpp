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

#include <dsn/tool-api/command_manager.h>
#include <dsn/dist/fmt_logging.h>
#include "app_balance_policy.h"
#include "meta_service.h"

namespace dsn {
namespace replication {

app_balance_policy::app_balance_policy(meta_service *svc)
    : load_balance_policy(svc),
      _ctrl_balancer_in_turn(nullptr),
      _ctrl_only_primary_balancer(nullptr),
      _ctrl_only_move_primary(nullptr)
{
    if (_svc != nullptr) {
        _balancer_in_turn = _svc->get_meta_options()._lb_opts.balancer_in_turn;
        _only_primary_balancer = _svc->get_meta_options()._lb_opts.only_primary_balancer;
        _only_move_primary = _svc->get_meta_options()._lb_opts.only_move_primary;
    } else {
        _balancer_in_turn = false;
        _only_primary_balancer = false;
        _only_move_primary = false;
    }
    register_ctrl_commands();
}

app_balance_policy::~app_balance_policy() { unregister_ctrl_commands(); }

void app_balance_policy::balance(bool checker, const meta_view *global_view, migration_list *list)
{
    init(global_view, list);
    const app_mapper &apps = *_global_view->apps;
    if (!execute_balance(apps,
                         checker,
                         _balancer_in_turn,
                         _only_move_primary,
                         std::bind(&app_balance_policy::primary_balance,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2))) {
        return;
    }

    if (!need_balance_secondaries(checker)) {
        return;
    }

    // we seperate the primary/secondary balancer for 2 reasons:
    // 1. globally primary balancer may make secondary unbalanced
    // 2. in one-by-one mode, a secondary balance decision for an app may be prior than
    // another app's primary balancer if not seperated.
    execute_balance(apps,
                    checker,
                    _balancer_in_turn,
                    _only_move_primary,
                    std::bind(&app_balance_policy::copy_secondary,
                              this,
                              std::placeholders::_1,
                              std::placeholders::_2));
}

void app_balance_policy::register_ctrl_commands()
{
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _ctrl_balancer_in_turn = dsn::command_manager::instance().register_command(
            {"meta.lb.balancer_in_turn"},
            "meta.lb.balancer_in_turn <true|false>",
            "control whether do app balancer in turn",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(_balancer_in_turn, "lb.balancer_in_turn", args);
            });

        _ctrl_only_primary_balancer = dsn::command_manager::instance().register_command(
            {"meta.lb.only_primary_balancer"},
            "meta.lb.only_primary_balancer <true|false>",
            "control whether do only primary balancer",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _only_primary_balancer, "lb.only_primary_balancer", args);
            });

        _ctrl_only_move_primary = dsn::command_manager::instance().register_command(
            {"meta.lb.only_move_primary"},
            "meta.lb.only_move_primary <true|false>",
            "control whether only move primary in balancer",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _only_move_primary, "lb.only_move_primary", args);
            });
    });
}

void app_balance_policy::unregister_ctrl_commands()
{
    UNREGISTER_VALID_HANDLER(_ctrl_balancer_in_turn);
    UNREGISTER_VALID_HANDLER(_ctrl_only_primary_balancer);
    UNREGISTER_VALID_HANDLER(_ctrl_only_move_primary);
}

bool app_balance_policy::need_balance_secondaries(bool balance_checker)
{
    if (!balance_checker && !_migration_result->empty()) {
        ddebug("stop to do secondary balance coz we already have actions to do");
        return false;
    }
    if (_only_primary_balancer) {
        ddebug("stop to do secondary balancer coz it is not allowed");
        return false;
    }
    return true;
}

bool app_balance_policy::copy_secondary(const std::shared_ptr<app_state> &app, bool place_holder)
{
    node_mapper &nodes = *(_global_view->nodes);
    const app_mapper &apps = *_global_view->apps;
    int replicas_low = app->partition_count / _alive_nodes;

    std::unique_ptr<copy_replica_operation> operation = dsn::make_unique<copy_secondary_operation>(
        app, apps, nodes, address_vec, address_id, replicas_low);
    return operation->start(_migration_result);
}

copy_secondary_operation::copy_secondary_operation(
    const std::shared_ptr<app_state> app,
    const app_mapper &apps,
    node_mapper &nodes,
    const std::vector<dsn::rpc_address> &address_vec,
    const std::unordered_map<dsn::rpc_address, int> &address_id,
    int replicas_low)
    : copy_replica_operation(app, apps, nodes, address_vec, address_id), _replicas_low(replicas_low)
{
}

bool copy_secondary_operation::can_continue()
{
    int id_min = *_ordered_address_ids.begin();
    int id_max = *_ordered_address_ids.rbegin();
    if (_partition_counts[id_max] <= _replicas_low ||
        _partition_counts[id_max] - _partition_counts[id_min] <= 1) {
        ddebug_f("{}: stop copy secondary coz it will be balanced later", _app->get_logname());
        return false;
    }
    return true;
}

int copy_secondary_operation::get_partition_count(const node_state &ns) const
{
    return ns.partition_count(_app->app_id);
}

bool copy_secondary_operation::can_select(gpid pid, migration_list *result)
{
    int id_max = *_ordered_address_ids.rbegin();
    const node_state &max_ns = _nodes.at(_address_vec[id_max]);
    if (max_ns.served_as(pid) == partition_status::PS_PRIMARY) {
        dinfo_f("{}: skip gpid({}.{}) coz it is primary",
                _app->get_logname(),
                pid.get_app_id(),
                pid.get_partition_index());
        return false;
    }

    // if the pid have been used
    if (result->find(pid) != result->end()) {
        dinfo_f("{}: skip gpid({}.{}) coz it is already copyed",
                _app->get_logname(),
                pid.get_app_id(),
                pid.get_partition_index());
        return false;
    }

    int id_min = *_ordered_address_ids.begin();
    const node_state &min_ns = _nodes.at(_address_vec[id_min]);
    if (min_ns.served_as(pid) != partition_status::PS_INACTIVE) {
        dinfo_f("{}: skip gpid({}.{}) coz it is already a member on the target node",
                _app->get_logname(),
                pid.get_app_id(),
                pid.get_partition_index());
        return false;
    }
    return true;
}

balance_type copy_secondary_operation::get_balance_type() { return balance_type::COPY_SECONDARY; }

} // namespace replication
} // namespace dsn
