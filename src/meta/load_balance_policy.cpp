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

#include "meta/load_balance_policy.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <limits.h>
#include <algorithm>
#include <iterator>
#include <limits>
#include <mutex>
#include <ostream>

#include "dsn.layer2_types.h"
#include "meta/greedy_load_balancer.h"
#include "meta/meta_data.h"
#include "meta_admin_types.h"
#include "utils/command_manager.h"
#include "utils/fail_point.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"

namespace dsn {
namespace replication {
DSN_DECLARE_uint64(min_live_node_count_for_unfreeze);

void dump_disk_load(app_id id, const rpc_address &node, bool only_primary, const disk_load &load)
{
    std::ostringstream load_string;
    load_string << std::endl << "<<<<<<<<<<" << std::endl;
    load_string << "load for " << node.to_string() << ", "
                << "app id: " << id;
    if (only_primary) {
        load_string << ", only for primary";
    }
    load_string << std::endl;

    for (const auto &kv : load) {
        load_string << kv.first << ": " << kv.second << std::endl;
    }
    load_string << ">>>>>>>>>>";
    LOG_DEBUG("{}", load_string.str());
}

bool calc_disk_load(node_mapper &nodes,
                    const app_mapper &apps,
                    app_id id,
                    const rpc_address &node,
                    bool only_primary,
                    /*out*/ disk_load &load)
{
    load.clear();
    const node_state *ns = get_node_state(nodes, node, false);
    CHECK_NOTNULL(ns, "can't find node({}) from node_state", node.to_string());

    auto add_one_replica_to_disk_load = [&](const gpid &pid) {
        LOG_DEBUG("add gpid({}) to node({}) disk load", pid, node);
        const config_context &cc = *get_config_context(apps, pid);
        auto iter = cc.find_from_serving(node);
        if (iter == cc.serving.end()) {
            LOG_WARNING(
                "can't collect gpid({})'s info from {}, which should be primary", pid, node);
            return false;
        } else {
            load[iter->disk_tag]++;
            return true;
        }
    };

    if (only_primary) {
        bool result = ns->for_each_primary(id, add_one_replica_to_disk_load);
        dump_disk_load(id, node, only_primary, load);
        return result;
    } else {
        bool result = ns->for_each_partition(id, add_one_replica_to_disk_load);
        dump_disk_load(id, node, only_primary, load);
        return result;
    }
}

std::unordered_map<dsn::rpc_address, disk_load>
get_node_loads(const std::shared_ptr<app_state> &app,
               const app_mapper &apps,
               node_mapper &nodes,
               bool only_primary)
{
    std::unordered_map<dsn::rpc_address, disk_load> node_loads;
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        if (!calc_disk_load(
                nodes, apps, app->app_id, iter->first, only_primary, node_loads[iter->first])) {
            LOG_WARNING(
                "stop the balancer as some replica infos aren't collected, node({}), app({})",
                iter->first.to_string(),
                app->get_logname());
            return node_loads;
        }
    }
    return node_loads;
}

const std::string &get_disk_tag(const app_mapper &apps, const rpc_address &node, const gpid &pid)
{
    const config_context &cc = *get_config_context(apps, pid);
    auto iter = cc.find_from_serving(node);
    CHECK(iter != cc.serving.end(), "can't find disk tag of gpid({}) for {}", pid, node);
    return iter->disk_tag;
}

std::shared_ptr<configuration_balancer_request>
generate_balancer_request(const app_mapper &apps,
                          const partition_configuration &pc,
                          const balance_type &type,
                          const rpc_address &from,
                          const rpc_address &to)
{
    FAIL_POINT_INJECT_F("generate_balancer_request",
                        [](absl::string_view name) { return nullptr; });

    configuration_balancer_request result;
    result.gpid = pc.pid;

    std::string ans;
    switch (type) {
    case balance_type::MOVE_PRIMARY:
        ans = "move_primary";
        result.balance_type = balancer_request_type::move_primary;
        result.action_list.emplace_back(
            new_proposal_action(from, from, config_type::CT_DOWNGRADE_TO_SECONDARY));
        result.action_list.emplace_back(
            new_proposal_action(to, to, config_type::CT_UPGRADE_TO_PRIMARY));
        break;
    case balance_type::COPY_PRIMARY:
        ans = "copy_primary";
        result.balance_type = balancer_request_type::copy_primary;
        result.action_list.emplace_back(
            new_proposal_action(from, to, config_type::CT_ADD_SECONDARY_FOR_LB));
        result.action_list.emplace_back(
            new_proposal_action(from, from, config_type::CT_DOWNGRADE_TO_SECONDARY));
        result.action_list.emplace_back(
            new_proposal_action(to, to, config_type::CT_UPGRADE_TO_PRIMARY));
        result.action_list.emplace_back(new_proposal_action(to, from, config_type::CT_REMOVE));
        break;
    case balance_type::COPY_SECONDARY:
        ans = "copy_secondary";
        result.balance_type = balancer_request_type::copy_secondary;
        result.action_list.emplace_back(
            new_proposal_action(pc.primary, to, config_type::CT_ADD_SECONDARY_FOR_LB));
        result.action_list.emplace_back(
            new_proposal_action(pc.primary, from, config_type::CT_REMOVE));
        break;
    default:
        CHECK(false, "");
    }
    LOG_INFO("generate balancer: {} {} from {} of disk_tag({}) to {}",
             pc.pid,
             ans,
             from,
             get_disk_tag(apps, from, pc.pid),
             to);
    return std::make_shared<configuration_balancer_request>(std::move(result));
}

load_balance_policy::load_balance_policy(meta_service *svc)
    : _svc(svc), _ctrl_balancer_ignored_apps(nullptr)
{
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _ctrl_balancer_ignored_apps = dsn::command_manager::instance().register_command(
            {"meta.lb.ignored_app_list"},
            "meta.lb.ignored_app_list <get|set|clear> [app_id1,app_id2..]",
            "get, set and clear balancer ignored_app_list",
            [this](const std::vector<std::string> &args) {
                return remote_command_balancer_ignored_app_ids(args);
            });
    });
}

load_balance_policy::~load_balance_policy() {}

void load_balance_policy::init(const meta_view *global_view, migration_list *list)
{
    _global_view = global_view;
    _migration_result = list;
    const node_mapper &nodes = *_global_view->nodes;
    _alive_nodes = nodes.size();
    number_nodes(nodes);
}

bool load_balance_policy::primary_balance(const std::shared_ptr<app_state> &app,
                                          bool only_move_primary)
{
    CHECK_GE_MSG(_alive_nodes,
                 FLAGS_min_live_node_count_for_unfreeze,
                 "too few alive nodes will lead to freeze");
    LOG_INFO("primary balancer for app({}:{})", app->app_name, app->app_id);

    auto graph = ford_fulkerson::builder(app, *_global_view->nodes, address_id).build();
    if (nullptr == graph) {
        LOG_DEBUG("the primaries are balanced for app({}:{})", app->app_name, app->app_id);
        return true;
    }

    auto path = graph->find_shortest_path();
    if (path != nullptr) {
        LOG_DEBUG("{} primaries are flew", path->_flow.back());
        return move_primary(std::move(path));
    } else {
        LOG_INFO("we can't make the server load more balanced by moving primaries to secondaries");
        if (!only_move_primary) {
            return copy_primary(app, graph->have_less_than_average());
        } else {
            LOG_INFO("stop to copy primary for app({}) coz it is disabled", app->get_logname());
            return true;
        }
    }
}

bool load_balance_policy::copy_primary(const std::shared_ptr<app_state> &app,
                                       bool still_have_less_than_average)
{
    node_mapper &nodes = *(_global_view->nodes);
    const app_mapper &apps = *_global_view->apps;
    int replicas_low = app->partition_count / _alive_nodes;

    std::unique_ptr<copy_replica_operation> operation = std::make_unique<copy_primary_operation>(
        app, apps, nodes, address_vec, address_id, still_have_less_than_average, replicas_low);
    return operation->start(_migration_result);
}

bool load_balance_policy::move_primary(std::unique_ptr<flow_path> path)
{
    // used to calculate the primary disk loads of each server.
    // disk_load[disk_tag] means how many primaies on this "disk_tag".
    // IF disk_load.find(disk_tag) == disk_load.end(), means 0
    disk_load loads[2];
    disk_load *prev_load = &loads[0];
    disk_load *current_load = &loads[1];
    node_mapper &nodes = *(_global_view->nodes);
    const app_mapper &apps = *(_global_view->apps);

    int current = path->_prev.back();
    if (!calc_disk_load(
            nodes, apps, path->_app->app_id, address_vec[current], true, *current_load)) {
        LOG_WARNING("stop move primary as some replica infos aren't collected, node({}), app({})",
                    address_vec[current].to_string(),
                    path->_app->get_logname());
        return false;
    }

    int plan_moving = path->_flow.back();
    while (path->_prev[current] != 0) {
        rpc_address from = address_vec[path->_prev[current]];
        rpc_address to = address_vec[current];
        if (!calc_disk_load(nodes, apps, path->_app->app_id, from, true, *prev_load)) {
            LOG_WARNING(
                "stop move primary as some replica infos aren't collected, node({}), app({})",
                from.to_string(),
                path->_app->get_logname());
            return false;
        }

        start_moving_primary(path->_app, from, to, plan_moving, prev_load, current_load);

        current = path->_prev[current];
        std::swap(current_load, prev_load);
    }
    return true;
}

void load_balance_policy::start_moving_primary(const std::shared_ptr<app_state> &app,
                                               const rpc_address &from,
                                               const rpc_address &to,
                                               int plan_moving,
                                               disk_load *prev_load,
                                               disk_load *current_load)
{
    std::list<dsn::gpid> potential_moving = calc_potential_moving(app, from, to);
    auto potential_moving_size = potential_moving.size();
    CHECK_LE_MSG(plan_moving,
                 potential_moving_size,
                 "from({}) to({}) plan({}), can_move({})",
                 from,
                 to,
                 plan_moving,
                 potential_moving_size);

    while (plan_moving-- > 0) {
        dsn::gpid selected = select_moving(potential_moving, prev_load, current_load, from, to);

        const partition_configuration &pc = app->partitions[selected.get_partition_index()];
        auto balancer_result = _migration_result->emplace(
            selected,
            generate_balancer_request(
                *_global_view->apps, pc, balance_type::MOVE_PRIMARY, from, to));
        CHECK(balancer_result.second, "gpid({}) already inserted as an action", selected);

        --(*prev_load)[get_disk_tag(*_global_view->apps, from, selected)];
        ++(*current_load)[get_disk_tag(*_global_view->apps, to, selected)];
    }
}

std::list<dsn::gpid> load_balance_policy::calc_potential_moving(
    const std::shared_ptr<app_state> &app, const rpc_address &from, const rpc_address &to)
{
    std::list<dsn::gpid> potential_moving;
    const node_state &ns = _global_view->nodes->find(from)->second;
    ns.for_each_primary(app->app_id, [&](const gpid &pid) {
        const partition_configuration &pc = app->partitions[pid.get_partition_index()];
        if (is_secondary(pc, to)) {
            potential_moving.push_back(pid);
        }
        return true;
    });
    return potential_moving;
}

dsn::gpid load_balance_policy::select_moving(std::list<dsn::gpid> &potential_moving,
                                             disk_load *prev_load,
                                             disk_load *current_load,
                                             rpc_address from,
                                             rpc_address to)
{
    std::list<dsn::gpid>::iterator selected = potential_moving.end();
    int max = std::numeric_limits<int>::min();

    for (auto it = potential_moving.begin(); it != potential_moving.end(); ++it) {
        int load_difference = (*prev_load)[get_disk_tag(*_global_view->apps, from, *it)] -
                              (*current_load)[get_disk_tag(*_global_view->apps, to, *it)];
        if (load_difference > max) {
            max = load_difference;
            selected = it;
        }
    }

    CHECK(selected != potential_moving.end(), "can't find gpid to move from({}) to({})", from, to);
    auto res = *selected;
    potential_moving.erase(selected);
    return res;
}

bool load_balance_policy::execute_balance(
    const app_mapper &apps,
    bool balance_checker,
    bool balance_in_turn,
    bool only_move_primary,
    const std::function<bool(const std::shared_ptr<app_state> &, bool)> &balance_operation)
{
    for (const auto &kv : apps) {
        const std::shared_ptr<app_state> &app = kv.second;
        if (is_ignored_app(kv.first)) {
            LOG_INFO("skip to do balance for the ignored app[{}]", app->get_logname());
            continue;
        }
        if (app->status != app_status::AS_AVAILABLE || app->is_bulk_loading || app->splitting())
            continue;

        bool enough_information = balance_operation(app, only_move_primary);
        if (!enough_information) {
            // Even if we don't have enough info for current app,
            // the decisions made by previous apps are kept.
            // t_migration_result->empty();
            return false;
        }
        if (!balance_checker) {
            if (!_migration_result->empty()) {
                if (balance_in_turn) {
                    LOG_INFO("stop to handle more apps after we found some actions for {}",
                             app->get_logname());
                    return false;
                }
            }
        }
    }
    return true;
}

std::string
load_balance_policy::remote_command_balancer_ignored_app_ids(const std::vector<std::string> &args)
{
    static const std::string invalid_arguments("invalid arguments");
    if (args.empty()) {
        return invalid_arguments;
    }
    if (args[0] == "set") {
        return set_balancer_ignored_app_ids(args);
    }
    if (args[0] == "get") {
        return get_balancer_ignored_app_ids();
    }
    if (args[0] == "clear") {
        return clear_balancer_ignored_app_ids();
    }
    return invalid_arguments;
}

std::string load_balance_policy::set_balancer_ignored_app_ids(const std::vector<std::string> &args)
{
    static const std::string invalid_arguments("invalid arguments");
    if (args.size() != 2) {
        return invalid_arguments;
    }

    std::vector<std::string> app_ids;
    dsn::utils::split_args(args[1].c_str(), app_ids, ',');
    if (app_ids.empty()) {
        return invalid_arguments;
    }

    std::set<app_id> app_list;
    for (const std::string &app_id_str : app_ids) {
        app_id app;
        if (!dsn::buf2int32(app_id_str, app)) {
            return invalid_arguments;
        }
        app_list.insert(app);
    }

    dsn::zauto_write_lock l(_balancer_ignored_apps_lock);
    _balancer_ignored_apps = std::move(app_list);
    return "set ok";
}

std::string load_balance_policy::get_balancer_ignored_app_ids()
{
    std::stringstream oss;
    dsn::zauto_read_lock l(_balancer_ignored_apps_lock);
    if (_balancer_ignored_apps.empty()) {
        return "no ignored apps";
    }
    oss << "ignored_app_id_list: ";
    std::copy(_balancer_ignored_apps.begin(),
              _balancer_ignored_apps.end(),
              std::ostream_iterator<app_id>(oss, ","));
    std::string app_ids = oss.str();
    app_ids[app_ids.size() - 1] = '\0';
    return app_ids;
}

std::string load_balance_policy::clear_balancer_ignored_app_ids()
{
    dsn::zauto_write_lock l(_balancer_ignored_apps_lock);
    _balancer_ignored_apps.clear();
    return "clear ok";
}

bool load_balance_policy::is_ignored_app(app_id app_id)
{
    dsn::zauto_read_lock l(_balancer_ignored_apps_lock);
    return _balancer_ignored_apps.find(app_id) != _balancer_ignored_apps.end();
}

void load_balance_policy::number_nodes(const node_mapper &nodes)
{
    int current_id = 1;

    address_id.clear();
    address_vec.resize(_alive_nodes + 2);
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        CHECK(!iter->first.is_invalid() && !iter->second.addr().is_invalid(), "invalid address");
        CHECK(iter->second.alive(), "dead node");

        address_id[iter->first] = current_id;
        address_vec[current_id] = iter->first;
        ++current_id;
    }
}

ford_fulkerson::ford_fulkerson(const std::shared_ptr<app_state> &app,
                               const node_mapper &nodes,
                               const std::unordered_map<dsn::rpc_address, int> &address_id,
                               uint32_t higher_count,
                               uint32_t lower_count,
                               int replicas_low)
    : _app(app),
      _nodes(nodes),
      _address_id(address_id),
      _higher_count(higher_count),
      _lower_count(lower_count),
      _replicas_low(replicas_low)
{
    make_graph();
}

// using dijstra to find shortest path
std::unique_ptr<flow_path> ford_fulkerson::find_shortest_path()
{
    std::vector<bool> visit(_graph_nodes, false);
    std::vector<int> flow(_graph_nodes, 0);
    std::vector<int> prev(_graph_nodes, -1);
    flow[0] = INT_MAX;
    while (!visit.back()) {
        auto pos = select_node(visit, flow);
        if (pos == -1) {
            break;
        }
        update_flow(pos, visit, _network, flow, prev);
    }

    if (visit.back() && flow.back() != 0) {
        return std::make_unique<struct flow_path>(_app, std::move(flow), std::move(prev));
    } else {
        return nullptr;
    }
}

void ford_fulkerson::make_graph()
{
    _graph_nodes = _nodes.size() + 2;
    _network.resize(_graph_nodes, std::vector<int>(_graph_nodes, 0));
    for (const auto &node : _nodes) {
        int node_id = _address_id.at(node.first);
        add_edge(node_id, node.second);
        update_decree(node_id, node.second);
    }
    handle_corner_case();
}

void ford_fulkerson::add_edge(int node_id, const node_state &ns)
{
    int primary_count = ns.primary_count(_app->app_id);
    if (primary_count > _replicas_low) {
        _network[0][node_id] = primary_count - _replicas_low;
    } else {
        _network[node_id].back() = _replicas_low - primary_count;
    }
}

void ford_fulkerson::update_decree(int node_id, const node_state &ns)
{
    ns.for_each_primary(_app->app_id, [&, this](const gpid &pid) {
        const partition_configuration &pc = _app->partitions[pid.get_partition_index()];
        for (const auto &secondary : pc.secondaries) {
            auto i = _address_id.find(secondary);
            CHECK(i != _address_id.end(), "invalid secondary address, address = {}", secondary);
            _network[node_id][i->second]++;
        }
        return true;
    });
}

void ford_fulkerson::handle_corner_case()
{
    // Suppose you have an 8-shard app in a cluster with 3 nodes(which name are node1, node2,
    // node3). The distribution of primaries among these nodes is as follow:
    // node1 : [0, 1, 2, 3]
    // node2 : [4, 5]
    // node2 : [6, 7]
    // This is obviously unbalanced.
    // But if we don't handle this corner case, primary migration will not be triggered
    if (_higher_count > 0 && _lower_count == 0) {
        for (int i = 0; i != _graph_nodes; ++i) {
            if (_network[0][i] > 0)
                --_network[0][i];
            else
                ++_network[i][_graph_nodes - 1];
        }
    }
}

int ford_fulkerson::select_node(std::vector<bool> &visit, const std::vector<int> &flow)
{
    auto pos = max_value_pos(visit, flow);
    if (pos != -1) {
        visit[pos] = true;
    }
    return pos;
}

int ford_fulkerson::max_value_pos(const std::vector<bool> &visit, const std::vector<int> &flow)
{
    int pos = -1, max_value = 0;
    for (auto i = 0; i != _graph_nodes; ++i) {
        if (!visit[i] && flow[i] > max_value) {
            pos = i;
            max_value = flow[i];
        }
    }
    return pos;
}

void ford_fulkerson::update_flow(int pos,
                                 const std::vector<bool> &visit,
                                 const std::vector<std::vector<int>> &network,
                                 std::vector<int> &flow,
                                 std::vector<int> &prev)
{
    for (auto i = 0; i != _graph_nodes; ++i) {
        if (visit[i]) {
            continue;
        }

        auto min = std::min(flow[pos], network[pos][i]);
        if (min > flow[i]) {
            flow[i] = min;
            prev[i] = pos;
        }
    }
}

copy_replica_operation::copy_replica_operation(
    const std::shared_ptr<app_state> app,
    const app_mapper &apps,
    node_mapper &nodes,
    const std::vector<dsn::rpc_address> &address_vec,
    const std::unordered_map<dsn::rpc_address, int> &address_id)
    : _app(app), _apps(apps), _nodes(nodes), _address_vec(address_vec), _address_id(address_id)
{
}

bool copy_replica_operation::start(migration_list *result)
{
    init_ordered_address_ids();
    _node_loads = get_node_loads(_app, _apps, _nodes, only_copy_primary());
    if (_node_loads.size() != _nodes.size()) {
        return false;
    }

    while (true) {
        if (!can_continue()) {
            break;
        }

        gpid selected_pid = select_partition(result);
        if (selected_pid.get_app_id() != -1) {
            copy_once(selected_pid, result);
            update_ordered_address_ids();
        } else {
            _ordered_address_ids.erase(--_ordered_address_ids.end());
        }
    }
    return true;
}

const partition_set *copy_replica_operation::get_all_partitions()
{
    int id_max = *_ordered_address_ids.rbegin();
    const node_state &ns = _nodes.find(_address_vec[id_max])->second;
    const partition_set *partitions = ns.partitions(_app->app_id, only_copy_primary());
    return partitions;
}

gpid copy_replica_operation::select_max_load_gpid(const partition_set *partitions,
                                                  migration_list *result)
{
    int id_max = *_ordered_address_ids.rbegin();
    const disk_load &load_on_max = _node_loads.at(_address_vec[id_max]);

    gpid selected_pid(-1, -1);
    int max_load = -1;
    for (const gpid &pid : *partitions) {
        if (!can_select(pid, result)) {
            continue;
        }

        const std::string &disk_tag = get_disk_tag(_apps, _address_vec[id_max], pid);
        auto load = load_on_max.at(disk_tag);
        if (load > max_load) {
            selected_pid = pid;
            max_load = load;
        }
    }
    return selected_pid;
}

void copy_replica_operation::copy_once(gpid selected_pid, migration_list *result)
{
    auto from = _address_vec[*_ordered_address_ids.rbegin()];
    auto to = _address_vec[*_ordered_address_ids.begin()];

    auto pc = _app->partitions[selected_pid.get_partition_index()];
    auto request = generate_balancer_request(_apps, pc, get_balance_type(), from, to);
    result->emplace(selected_pid, request);
}

void copy_replica_operation::update_ordered_address_ids()
{
    int id_min = *_ordered_address_ids.begin();
    int id_max = *_ordered_address_ids.rbegin();
    --_partition_counts[id_max];
    ++_partition_counts[id_min];

    _ordered_address_ids.erase(_ordered_address_ids.begin());
    _ordered_address_ids.erase(--_ordered_address_ids.end());

    _ordered_address_ids.insert(id_max);
    _ordered_address_ids.insert(id_min);
}

void copy_replica_operation::init_ordered_address_ids()
{
    _partition_counts.resize(_address_vec.size(), 0);
    for (const auto &iter : _nodes) {
        auto id = _address_id.at(iter.first);
        _partition_counts[id] = get_partition_count(iter.second);
    }

    std::set<int, std::function<bool(int left, int right)>> ordered_queue(
        [this](int left, int right) {
            return _partition_counts[left] != _partition_counts[right]
                       ? _partition_counts[left] < _partition_counts[right]
                       : left < right;
        });
    for (const auto &iter : _nodes) {
        auto id = _address_id.at(iter.first);
        ordered_queue.insert(id);
    }
    _ordered_address_ids.swap(ordered_queue);
}

gpid copy_replica_operation::select_partition(migration_list *result)
{
    const partition_set *partitions = get_all_partitions();

    int id_max = *_ordered_address_ids.rbegin();
    const node_state &ns = _nodes.find(_address_vec[id_max])->second;
    CHECK(partitions != nullptr && !partitions->empty(),
          "max load({}) shouldn't empty",
          ns.addr().to_string());

    return select_max_load_gpid(partitions, result);
}

copy_primary_operation::copy_primary_operation(
    const std::shared_ptr<app_state> app,
    const app_mapper &apps,
    node_mapper &nodes,
    const std::vector<dsn::rpc_address> &address_vec,
    const std::unordered_map<dsn::rpc_address, int> &address_id,
    bool have_lower_than_average,
    int replicas_low)
    : copy_replica_operation(app, apps, nodes, address_vec, address_id)
{
    _have_lower_than_average = have_lower_than_average;
    _replicas_low = replicas_low;
}

int copy_primary_operation::get_partition_count(const node_state &ns) const
{
    return ns.primary_count(_app->app_id);
}

bool copy_primary_operation::can_select(gpid pid, migration_list *result)
{
    return result->find(pid) == result->end();
}

bool copy_primary_operation::can_continue()
{
    int id_min = *_ordered_address_ids.begin();
    if (_have_lower_than_average && _partition_counts[id_min] >= _replicas_low) {
        LOG_INFO("{}: stop the copy due to primaries on all nodes will reach low later.",
                 _app->get_logname());
        return false;
    }

    int id_max = *_ordered_address_ids.rbegin();
    if (!_have_lower_than_average && _partition_counts[id_max] - _partition_counts[id_min] <= 1) {
        LOG_INFO("{}: stop the copy due to the primary will be balanced later.",
                 _app->get_logname());
        return false;
    }
    return true;
}

enum balance_type copy_primary_operation::get_balance_type() { return balance_type::COPY_PRIMARY; }
} // namespace replication
} // namespace dsn
