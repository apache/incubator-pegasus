/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     a greedy load balancer implementation
 *
 * Revision history:
 *     2016-02-3, Weijie Sun(sunweijie@xiaomi.com), first version
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), refactor, now the balancer only have
 * strategy
 */

#include <algorithm>
#include <iostream>
#include <queue>
#include <dsn/tool-api/command_manager.h>
#include <dsn/utility/math.h>
#include "greedy_load_balancer.h"
#include "meta_data.h"

namespace dsn {
namespace replication {

greedy_load_balancer::greedy_load_balancer(meta_service *_svc)
    : simple_load_balancer(_svc),
      _ctrl_balancer_in_turn(nullptr),
      _ctrl_only_primary_balancer(nullptr),
      _ctrl_only_move_primary(nullptr),
      _get_balance_operation_count(nullptr)
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

    ::memset(t_operation_counters, 0, sizeof(t_operation_counters));

    // init perf counters
    _balance_operation_count.init_app_counter("eon.greedy_balancer",
                                              "balance_operation_count",
                                              COUNTER_TYPE_NUMBER,
                                              "balance operation count to be done");
    _recent_balance_move_primary_count.init_app_counter(
        "eon.greedy_balancer",
        "recent_balance_move_primary_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "move primary count by balancer in the recent period");
    _recent_balance_copy_primary_count.init_app_counter(
        "eon.greedy_balancer",
        "recent_balance_copy_primary_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "copy primary count by balancer in the recent period");
    _recent_balance_copy_secondary_count.init_app_counter(
        "eon.greedy_balancer",
        "recent_balance_copy_secondary_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "copy secondary count by balancer in the recent period");
}

greedy_load_balancer::~greedy_load_balancer()
{
    UNREGISTER_VALID_HANDLER(_ctrl_balancer_in_turn);
    UNREGISTER_VALID_HANDLER(_ctrl_only_primary_balancer);
    UNREGISTER_VALID_HANDLER(_ctrl_only_move_primary);
    UNREGISTER_VALID_HANDLER(_get_balance_operation_count);
}

void greedy_load_balancer::register_ctrl_commands()
{
    // register command that belong to simple_load_balancer
    simple_load_balancer::register_ctrl_commands();

    _ctrl_balancer_in_turn = dsn::command_manager::instance().register_app_command(
        {"lb.balancer_in_turn"},
        "lb.balancer_in_turn <true|false>",
        "control whether do app balancer in turn",
        [this](const std::vector<std::string> &args) {
            HANDLE_CLI_FLAGS(_balancer_in_turn, args);
        });

    _ctrl_only_primary_balancer = dsn::command_manager::instance().register_app_command(
        {"lb.only_primary_balancer"},
        "lb.only_primary_balancer <true|false>",
        "control whether do only primary balancer",
        [this](const std::vector<std::string> &args) {
            HANDLE_CLI_FLAGS(_only_primary_balancer, args);
        });

    _ctrl_only_move_primary = dsn::command_manager::instance().register_app_command(
        {"lb.only_move_primary"},
        "lb.only_move_primary <true|false>",
        "control whether only move primary in balancer",
        [this](const std::vector<std::string> &args) {
            HANDLE_CLI_FLAGS(_only_move_primary, args);
        });

    _get_balance_operation_count = dsn::command_manager::instance().register_app_command(
        {"lb.get_balance_operation_count"},
        "lb.get_balance_operation_count [total | move_pri | copy_pri | copy_sec | detail]",
        "get balance operation count",
        [this](const std::vector<std::string> &args) { return get_balance_operation_count(args); });
}

void greedy_load_balancer::unregister_ctrl_commands()
{
    UNREGISTER_VALID_HANDLER(_ctrl_balancer_in_turn);
    UNREGISTER_VALID_HANDLER(_ctrl_only_primary_balancer);
    UNREGISTER_VALID_HANDLER(_ctrl_only_move_primary);
    UNREGISTER_VALID_HANDLER(_get_balance_operation_count);

    simple_load_balancer::unregister_ctrl_commands();
}

std::string greedy_load_balancer::get_balance_operation_count(const std::vector<std::string> &args)
{
    if (args.empty()) {
        return std::string("total=" + std::to_string(t_operation_counters[ALL_COUNT]));
    }

    if (args[0] == "total") {
        return std::string("total=" + std::to_string(t_operation_counters[ALL_COUNT]));
    }

    std::string result("unknown");
    if (args[0] == "move_pri")
        result = std::string("move_pri=" + std::to_string(t_operation_counters[MOVE_PRI_COUNT]));
    else if (args[0] == "copy_pri")
        result = std::string("copy_pri=" + std::to_string(t_operation_counters[COPY_PRI_COUNT]));
    else if (args[0] == "copy_sec")
        result = std::string("copy_sec=" + std::to_string(t_operation_counters[COPY_SEC_COUNT]));
    else if (args[0] == "detail")
        result = std::string("move_pri=" + std::to_string(t_operation_counters[MOVE_PRI_COUNT]) +
                             ",copy_pri=" + std::to_string(t_operation_counters[COPY_PRI_COUNT]) +
                             ",copy_sec=" + std::to_string(t_operation_counters[COPY_SEC_COUNT]) +
                             ",total=" + std::to_string(t_operation_counters[ALL_COUNT]));
    else
        result = std::string("ERR: invalid arguments");

    return result;
}

void greedy_load_balancer::score(meta_view view, double &primary_stddev, double &total_stddev)
{
    // Calculate stddev of primary and partition count for current meta-view
    std::vector<uint32_t> primary_count;
    std::vector<uint32_t> partition_count;

    primary_stddev = 0.0;
    total_stddev = 0.0;

    bool primary_partial_sample = false;
    bool partial_sample = false;

    for (auto iter = view.nodes->begin(); iter != view.nodes->end(); ++iter) {
        const node_state &node = iter->second;
        if (node.alive()) {
            if (node.partition_count() != 0) {
                primary_count.emplace_back(node.primary_count());
                partition_count.emplace_back(node.partition_count());
            }
        } else {
            if (node.primary_count() != 0) {
                primary_partial_sample = true;
            }
            if (node.partition_count() != 0) {
                partial_sample = true;
            }
        }
    }

    if (primary_count.size() <= 1 || partition_count.size() <= 1)
        return;

    primary_stddev = utils::mean_stddev(primary_count, primary_partial_sample);
    total_stddev = utils::mean_stddev(partition_count, partial_sample);
}

std::shared_ptr<configuration_balancer_request>
greedy_load_balancer::generate_balancer_request(const partition_configuration &pc,
                                                const balance_type &type,
                                                const rpc_address &from,
                                                const rpc_address &to)
{
    configuration_balancer_request result;
    result.gpid = pc.pid;

    std::string ans;
    switch (type) {
    case balance_type::move_primary:
        ans = "move_primary";
        result.balance_type = balancer_request_type::move_primary;
        result.action_list.emplace_back(
            configuration_proposal_action{from, from, config_type::CT_DOWNGRADE_TO_SECONDARY});
        result.action_list.emplace_back(
            configuration_proposal_action{to, to, config_type::CT_UPGRADE_TO_PRIMARY});
        break;
    case balance_type::copy_primary:
        ans = "copy_primary";
        result.balance_type = balancer_request_type::copy_primary;
        result.action_list.emplace_back(
            configuration_proposal_action{from, to, config_type::CT_ADD_SECONDARY_FOR_LB});
        result.action_list.emplace_back(
            configuration_proposal_action{from, from, config_type::CT_DOWNGRADE_TO_SECONDARY});
        result.action_list.emplace_back(
            configuration_proposal_action{to, to, config_type::CT_UPGRADE_TO_PRIMARY});
        result.action_list.emplace_back(
            configuration_proposal_action{to, from, config_type::CT_REMOVE});
        break;
    case balance_type::copy_secondary:
        ans = "copy_secondary";
        result.balance_type = balancer_request_type::copy_secondary;
        result.action_list.emplace_back(
            configuration_proposal_action{pc.primary, to, config_type::CT_ADD_SECONDARY_FOR_LB});
        result.action_list.emplace_back(
            configuration_proposal_action{pc.primary, from, config_type::CT_REMOVE});
        break;
    default:
        dassert(false, "");
    }
    ddebug("generate balancer: %d.%d %s from %s of disk_tag(%s) to %s",
           pc.pid.get_app_id(),
           pc.pid.get_partition_index(),
           ans.c_str(),
           from.to_string(),
           get_disk_tag(from, pc.pid).c_str(),
           to.to_string());
    return std::make_shared<configuration_balancer_request>(std::move(result));
}

const std::string &greedy_load_balancer::get_disk_tag(const rpc_address &node, const gpid &pid)
{
    config_context &cc = *get_config_context(*(t_global_view->apps), pid);
    auto iter = cc.find_from_serving(node);
    dassert(iter != cc.serving.end(),
            "can't find disk tag of gpid(%d.%d) for %s",
            pid.get_app_id(),
            pid.get_partition_index(),
            node.to_string());
    return iter->disk_tag;
}

// assume all nodes are alive
bool greedy_load_balancer::copy_primary_per_app(const std::shared_ptr<app_state> &app,
                                                bool still_have_less_than_average,
                                                int replicas_low)
{
    const node_mapper &nodes = *(t_global_view->nodes);
    std::vector<int> future_primaries(address_vec.size(), 0);
    std::unordered_map<dsn::rpc_address, disk_load> node_loads;

    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        future_primaries[address_id[iter->first]] = iter->second.primary_count(app->app_id);
        if (!calc_disk_load(app->app_id, iter->first, true, node_loads[iter->first])) {
            dwarn("stop the balancer as some replica infos aren't collected, node(%s), app(%s)",
                  iter->first.to_string(),
                  app->get_logname());
            return false;
        }
    }

    std::set<int, std::function<bool(int a, int b)>> pri_queue([&future_primaries](int a, int b) {
        return future_primaries[a] != future_primaries[b]
                   ? future_primaries[a] < future_primaries[b]
                   : a < b;
    });
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        pri_queue.insert(address_id[iter->first]);
    }

    ddebug("start to do copy primary for app(%s), expected minimal primaries(%d), %s all have "
           "reached the value",
           app->get_logname(),
           replicas_low,
           still_have_less_than_average ? "not" : "");
    while (true) {
        int id_min = *pri_queue.begin();
        int id_max = *pri_queue.rbegin();

        if (still_have_less_than_average && future_primaries[id_min] >= replicas_low) {
            ddebug("%s: stop the copy due to primaries on all nodes will reach low later.",
                   app->get_logname());
            break;
        }
        if (!still_have_less_than_average &&
            future_primaries[id_max] - future_primaries[id_min] <= 1) {
            ddebug("%s: stop the copy due to the primary will be balanced later.",
                   app->get_logname());
            break;
        }

        ddebug("server with min/max load of app(%s): (%s have %d) vs (%s have %d)",
               app->get_logname(),
               address_vec[id_min].to_string(),
               future_primaries[id_min],
               address_vec[id_max].to_string(),
               future_primaries[id_max]);

        pri_queue.erase(pri_queue.begin());
        pri_queue.erase(--pri_queue.end());

        dassert(id_min != id_max,
                "min(%d) load and max(%d) load machines shouldn't the same",
                id_min,
                id_max);

        const node_state &ns = nodes.find(address_vec[id_max])->second;
        const partition_set *pri = ns.partitions(app->app_id, true);
        disk_load &load_on_max = node_loads[address_vec[id_max]];

        dassert(
            pri != nullptr && !pri->empty(), "max load(%s) shouldn't empty", ns.addr().to_string());

        // select a primary on id_max and copy it to id_min.
        // the selected primary should on a disk which have
        // most amount of primaries for current app.
        gpid selected_pid = {-1, -1};
        int *selected_load = nullptr;
        for (const gpid &pid : *pri) {
            if (t_migration_result->find(pid) == t_migration_result->end()) {
                const std::string &dtag = get_disk_tag(address_vec[id_max], pid);
                if (selected_load == nullptr || load_on_max[dtag] > *selected_load) {
                    dinfo("%s: select gpid(%d.%d) on disk(%s), load(%d)",
                          app->get_logname(),
                          pid.get_app_id(),
                          pid.get_partition_index(),
                          dtag.c_str(),
                          load_on_max[dtag]);
                    selected_pid = pid;
                    selected_load = &load_on_max[dtag];
                }
            }
        }

        dassert(selected_pid.get_app_id() != -1 && selected_load != nullptr,
                "can't find primry to copy from(%s) to(%s)",
                address_vec[id_max].to_string(),
                address_vec[id_min].to_string());
        const partition_configuration &pc = app->partitions[selected_pid.get_partition_index()];
        dassert(!is_member(pc, address_vec[id_min]),
                "gpid(%d.%d) can move from %s to %s",
                pc.pid.get_app_id(),
                pc.pid.get_partition_index(),
                address_vec[id_max].to_string(),
                address_vec[id_min].to_string());

        t_migration_result->emplace(
            selected_pid,
            generate_balancer_request(
                pc, balance_type::copy_primary, address_vec[id_max], address_vec[id_min]));

        --(*selected_load);
        --future_primaries[id_max];
        ++future_primaries[id_min];

        pri_queue.insert(id_max);
        pri_queue.insert(id_min);
    }

    return true;
}

bool greedy_load_balancer::copy_secondary_per_app(const std::shared_ptr<app_state> &app)
{
    std::vector<int> future_partitions(address_vec.size(), 0);
    std::vector<disk_load> node_loads(address_vec.size());

    int total_partitions = 0;
    for (const auto &pair : *(t_global_view->nodes)) {
        const node_state &ns = pair.second;
        future_partitions[address_id[ns.addr()]] = ns.partition_count(app->app_id);
        total_partitions += ns.partition_count(app->app_id);

        if (!calc_disk_load(app->app_id, ns.addr(), false, node_loads[address_id[ns.addr()]])) {
            dwarn("stop copy secondary as some replica infos aren't collected, node(%s), app(%s)",
                  ns.addr().to_string(),
                  app->get_logname());
            return false;
        }
    }

    std::set<int, std::function<bool(int, int)>> pri_queue([&future_partitions](int id1, int id2) {
        if (future_partitions[id1] != future_partitions[id2])
            return future_partitions[id1] < future_partitions[id2];
        return id1 < id2;
    });
    for (const auto &kv : address_id) {
        pri_queue.emplace(kv.second);
    }

    int replicas_low = total_partitions / t_alive_nodes;
    ddebug("%s: copy secondary, expected minimal replicas(%d) on nodes",
           app->get_logname(),
           replicas_low);
    while (true) {
        int min_id = *pri_queue.begin();
        int max_id = *pri_queue.rbegin();
        dassert(future_partitions[min_id] <= replicas_low,
                "%d VS %d",
                future_partitions[min_id],
                replicas_low);
        if (future_partitions[max_id] <= replicas_low ||
            future_partitions[max_id] - future_partitions[min_id] <= 1) {
            ddebug("%s: stop copy secondary coz it will be balanced later", app->get_logname());
            break;
        }

        pri_queue.erase(pri_queue.begin());
        pri_queue.erase(--pri_queue.end());

        const node_state &min_ns = (*(t_global_view->nodes))[address_vec[min_id]];
        const node_state &max_ns = (*(t_global_view->nodes))[address_vec[max_id]];
        const partition_set *all_partitions_max_load = max_ns.partitions(app->app_id, false);

        ddebug("%s: server with min/max load: (%s have %d), (%s have %d)",
               app->get_logname(),
               address_vec[min_id].to_string(),
               future_partitions[min_id],
               address_vec[max_id].to_string(),
               future_partitions[max_id]);

        int *selected_load = nullptr;
        gpid selected_pid;
        for (const gpid &pid : *all_partitions_max_load) {
            if (max_ns.served_as(pid) == partition_status::PS_PRIMARY) {
                dinfo("%s: skip gpid(%d.%d) coz it is primary",
                      app->get_logname(),
                      pid.get_app_id(),
                      pid.get_partition_index());
                continue;
            }
            // if the pid have been used
            if (t_migration_result->find(pid) != t_migration_result->end()) {
                dinfo("%s: skip gpid(%d.%d) coz it is already copyed",
                      app->get_logname(),
                      pid.get_app_id(),
                      pid.get_partition_index());
                continue;
            }
            if (min_ns.served_as(pid) != partition_status::PS_INACTIVE) {
                dinfo("%s: skip gpid(%d.%d) coz it is already a member on the target node",
                      app->get_logname(),
                      pid.get_app_id(),
                      pid.get_partition_index());
                continue;
            }

            int &load = node_loads[max_id][get_disk_tag(max_ns.addr(), pid)];
            if (selected_load == nullptr || *selected_load < load) {
                dinfo("%s: select gpid(%d.%d) as target, tag(%s), load(%d)",
                      app->get_logname(),
                      pid.get_app_id(),
                      pid.get_partition_index(),
                      get_disk_tag(max_ns.addr(), pid).c_str(),
                      load);
                selected_load = &load;
                selected_pid = pid;
            }
        }

        if (selected_load == nullptr) {
            ddebug("can't find partition to copy from %s to %s",
                   max_ns.addr().to_string(),
                   min_ns.addr().to_string());
            pri_queue.insert(min_id);
        } else {
            t_migration_result->emplace(
                selected_pid,
                generate_balancer_request(app->partitions[selected_pid.get_partition_index()],
                                          balance_type::copy_secondary,
                                          max_ns.addr(),
                                          min_ns.addr()));
            --(*selected_load);
            ++future_partitions[min_id];
            --future_partitions[max_id];
            pri_queue.insert(min_id);
            pri_queue.insert(max_id);
        }
    }

    return true;
}

void greedy_load_balancer::number_nodes(const node_mapper &nodes)
{
    int current_id = 1;

    address_id.clear();
    address_vec.resize(t_alive_nodes + 2);
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        dassert(!iter->first.is_invalid() && !iter->second.addr().is_invalid(), "invalid address");
        dassert(iter->second.alive(), "dead node");

        address_id[iter->first] = current_id;
        address_vec[current_id] = iter->first;
        ++current_id;
    }
}

void greedy_load_balancer::dump_disk_load(app_id id,
                                          const rpc_address &node,
                                          bool only_primary,
                                          const disk_load &load)
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
    dinfo("%s", load_string.str().c_str());
}

bool greedy_load_balancer::calc_disk_load(app_id id,
                                          const rpc_address &node,
                                          bool only_primary,
                                          /*out*/ disk_load &load)
{
    load.clear();
    const node_state *ns = get_node_state(*(t_global_view->nodes), node, false);
    dassert(ns != nullptr, "can't find node(%s) from node_state", node.to_string());

    auto add_one_replica_to_disk_load = [&, this](const gpid &pid) {
        dinfo("add gpid(%d.%d) to node(%s) disk load",
              pid.get_app_id(),
              pid.get_partition_index(),
              node.to_string());
        config_context &cc = *get_config_context(*(t_global_view->apps), pid);
        auto iter = cc.find_from_serving(node);
        if (iter == cc.serving.end()) {
            dwarn("can't collect gpid(%d.%d)'s info from %s, which should be primary",
                  pid.get_app_id(),
                  pid.get_partition_index(),
                  node.to_string());
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

bool greedy_load_balancer::move_primary_based_on_flow_per_app(const std::shared_ptr<app_state> &app,
                                                              const std::vector<int> &prev,
                                                              const std::vector<int> &flow)
{
    int graph_nodes = prev.size();
    int current = prev[graph_nodes - 1];

    // used to calculate the primary disk loads of each server.
    // disk_load[disk_tag] means how many primaies on this "disk_tag".
    // IF disk_load.find(disk_tag) == disk_load.end(), means 0
    disk_load loads[2];
    disk_load *prev_load = &loads[0];
    disk_load *current_load = &loads[1];

    if (!calc_disk_load(app->app_id, address_vec[current], true, *current_load)) {
        dwarn("stop move primary as some replica infos aren't collected, node(%s), app(%s)",
              address_vec[current].to_string(),
              app->get_logname());
        return false;
    }

    migration_list ml_this_turn;
    while (prev[current] != 0) {
        rpc_address from = address_vec[prev[current]];
        rpc_address to = address_vec[current];

        if (!calc_disk_load(app->app_id, from, true, *prev_load)) {
            dwarn("stop move primary as some replica infos aren't collected, node(%s), app(%s)",
                  from.to_string(),
                  app->get_logname());
            return false;
        }

        const node_state &ns = t_global_view->nodes->find(from)->second;
        std::list<dsn::gpid> potential_moving;
        int potential_moving_size = 0;
        ns.for_each_primary(app->app_id, [&](const gpid &pid) {
            const partition_configuration &pc = app->partitions[pid.get_partition_index()];
            if (is_secondary(pc, to)) {
                potential_moving.push_back(pid);
                potential_moving_size++;
            }
            return true;
        });

        int plan_moving = flow[graph_nodes - 1];
        dassert(plan_moving <= potential_moving_size,
                "from(%s) to(%s) plan(%d), can_move(%d)",
                from.to_string(),
                to.to_string(),
                plan_moving,
                potential_moving_size);

        while (plan_moving > 0) {
            std::list<dsn::gpid>::iterator selected = potential_moving.end();
            int selected_score = std::numeric_limits<int>::min();

            for (std::list<dsn::gpid>::iterator it = potential_moving.begin();
                 it != potential_moving.end();
                 ++it) {
                const config_context &cc = app->helpers->contexts[it->get_partition_index()];
                int score =
                    (*prev_load)[get_disk_tag(from, *it)] - (*current_load)[get_disk_tag(to, *it)];
                if (score > selected_score) {
                    selected_score = score;
                    selected = it;
                }
            }

            dassert(selected != potential_moving.end(),
                    "can't find gpid to move from(%s) to(%s)",
                    from.to_string(),
                    to.to_string());
            const partition_configuration &pc = app->partitions[selected->get_partition_index()];
            auto balancer_result = ml_this_turn.emplace(
                *selected, generate_balancer_request(pc, balance_type::move_primary, from, to));
            dassert(balancer_result.second,
                    "gpid(%d.%d) already inserted as an action",
                    (*selected).get_app_id(),
                    (*selected).get_partition_index());

            --(*prev_load)[get_disk_tag(from, *selected)];
            ++(*current_load)[get_disk_tag(to, *selected)];
            potential_moving.erase(selected);
            --plan_moving;
        }

        current = prev[current];
        std::swap(current_load, prev_load);
    }

    for (auto &kv : ml_this_turn) {
        auto r = t_migration_result->emplace(kv.first, kv.second);
        dassert(r.second,
                "gpid(%d.%d) already inserted as an action",
                kv.first.get_app_id(),
                kv.first.get_partition_index());
    }
    return true;
}

// shortest path based on dijstra, to find an augmenting path
void greedy_load_balancer::shortest_path(std::vector<bool> &visit,
                                         std::vector<int> &flow,
                                         std::vector<int> &prev,
                                         std::vector<std::vector<int>> &network)
{
    int pos, max_value;
    flow[0] = INT_MAX;

    int graph_nodes = network.size();
    while (!visit[graph_nodes - 1]) {
        pos = -1, max_value = 0;
        for (int i = 0; i != graph_nodes; ++i) {
            if (visit[i] == false && flow[i] > max_value) {
                pos = i;
                max_value = flow[i];
            }
        }

        if (pos == -1)
            break;

        visit[pos] = true;
        for (int i = 0; i != graph_nodes; ++i) {
            if (!visit[i] && std::min(flow[pos], network[pos][i]) > flow[i]) {
                flow[i] = std::min(flow[pos], network[pos][i]);
                prev[i] = pos;
            }
        }
    }
}

// load balancer based on ford-fulkerson
bool greedy_load_balancer::primary_balancer_per_app(const std::shared_ptr<app_state> &app)
{
    dassert(t_alive_nodes > 2, "too few alive nodes will lead to freeze");
    ddebug("primary balancer for app(%s:%d)", app->app_name.c_str(), app->app_id);

    const node_mapper &nodes = *(t_global_view->nodes);
    int replicas_low = app->partition_count / t_alive_nodes;
    int replicas_high = (app->partition_count + t_alive_nodes - 1) / t_alive_nodes;

    int lower_count = 0, higher_count = 0;
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        int c = iter->second.primary_count(app->app_id);
        if (c > replicas_high)
            higher_count++;
        else if (c < replicas_low)
            lower_count++;
    }
    if (higher_count == 0 && lower_count == 0) {
        dinfo("the primaries are balanced for app(%s:%d)", app->app_name.c_str(), app->app_id);
        return true;
    }

    size_t graph_nodes = t_alive_nodes + 2;
    std::vector<std::vector<int>> network(graph_nodes, std::vector<int>(graph_nodes, 0));

    // make graph
    for (auto iter = nodes.begin(); iter != nodes.end(); ++iter) {
        int from = address_id[iter->first];
        const node_state &ns = iter->second;
        int c = ns.primary_count(app->app_id);
        if (c > replicas_low)
            network[0][from] = c - replicas_low;
        else
            network[from][graph_nodes - 1] = replicas_low - c;

        ns.for_each_primary(app->app_id, [&, this](const gpid &pid) {
            const partition_configuration &pc = app->partitions[pid.get_partition_index()];
            for (auto &target : pc.secondaries) {
                auto i = address_id.find(target);
                dassert(i != address_id.end(),
                        "invalid secondary address, address = %s",
                        target.to_string());
                network[from][i->second]++;
            }
            return true;
        });
    }

    if (higher_count > 0 && lower_count == 0) {
        for (int i = 0; i != graph_nodes; ++i) {
            if (network[0][i] > 0)
                --network[0][i];
            else
                ++network[i][graph_nodes - 1];
        }
    }
    dinfo("%s: start to move primary", app->get_logname());
    std::vector<bool> visit(graph_nodes, false);
    std::vector<int> flow(graph_nodes, 0);
    std::vector<int> prev(graph_nodes, -1);

    shortest_path(visit, flow, prev, network);
    // we can't make the server load more balanced
    // by moving primaries to secondaries
    if (!visit[graph_nodes - 1] || flow[graph_nodes - 1] == 0) {
        if (!_only_move_primary) {
            return copy_primary_per_app(app, lower_count != 0, replicas_low);
        } else {
            ddebug("stop to move primary for app(%s) coz it is disabled", app->get_logname());
            return true;
        }
    }

    dinfo("%u primaries are flew", flow[graph_nodes - 1]);
    return move_primary_based_on_flow_per_app(app, prev, flow);
}

bool greedy_load_balancer::primary_balancer_globally()
{
    ddebug("greedy primary balancer globally");
    std::vector<int> primary_count(address_vec.size(), 0);
    dsn::replication::for_each_available_app(
        *t_global_view->apps, [&, this](const std::shared_ptr<app_state> &app) {
            int max_value = -1;
            int min_value = std::numeric_limits<int>::max();
            for (const auto &np : *(t_global_view->nodes)) {
                int id = address_id[np.first];
                primary_count[id] += np.second.primary_count(app->app_id);
                max_value = std::max(max_value, primary_count[id]);
                min_value = std::min(min_value, primary_count[id]);
            }
            dassert(max_value - min_value <= 2,
                    "invalid max(%d), min(%d), current_app(%d)",
                    max_value,
                    min_value,
                    app->app_id);
            if (max_value - min_value <= 1)
                return true;

            // BFS to find a path from the max_value set to the min_value_set
            std::vector<std::set<dsn::gpid>> used_primaries(address_vec.size());
            for (int i = 1; i <= t_alive_nodes; ++i) {
                if (primary_count[i] != max_value)
                    continue;

                std::vector<dsn::gpid> bfs_prev(address_vec.size(), dsn::gpid(-1, -1));
                std::queue<int> bfs_q;
                bfs_q.push(i);
                bfs_prev[i] = {0, 0};

                while (!bfs_q.empty()) {
                    int id = bfs_q.front();
                    if (primary_count[id] == min_value)
                        break;

                    bfs_q.pop();
                    const node_state &ns = (*t_global_view->nodes)[address_vec[id]];
                    const partition_set *pri_set = ns.partitions(app->app_id, true);
                    if (pri_set == nullptr)
                        continue;
                    for (const gpid &pid : *pri_set) {
                        if (used_primaries[id].find(pid) != used_primaries[id].end())
                            continue;
                        const partition_configuration &pc =
                            app->partitions[pid.get_partition_index()];
                        dassert(pc.primary == address_vec[id],
                                "invalid primary address, %s VS %s",
                                pc.primary.to_string(),
                                address_vec[id].to_string());
                        for (const dsn::rpc_address &addr : pc.secondaries) {
                            int id2 = address_id[addr];
                            if (bfs_prev[id2].get_app_id() == -1) {
                                bfs_prev[id2] = pid;
                                bfs_q.push(id2);
                            }
                        }
                    }
                }

                if (!bfs_q.empty()) {
                    int id = bfs_q.front();
                    primary_count[id]++;
                    primary_count[i]--;

                    while (bfs_prev[id].get_app_id() != 0) {
                        dsn::gpid pid = bfs_prev[id];
                        const partition_configuration &pc =
                            app->partitions[pid.get_partition_index()];
                        t_migration_result->emplace(
                            pid,
                            generate_balancer_request(
                                pc, balance_type::move_primary, pc.primary, address_vec[id]));
                        // jump to prev node
                        id = address_id[pc.primary];
                        used_primaries[id].emplace(pid);
                    }

                    dassert(id == i, "expect first node(%d), actual (%d)", i, id);
                }
            }

            if (t_migration_result->empty()) {
                std::vector<int> node_id(t_alive_nodes);
                for (int i = 0; i < t_alive_nodes; ++i)
                    node_id[i] = i + 1;
                std::sort(node_id.begin(), node_id.end(), [&primary_count](int id1, int id2) {
                    return primary_count[id1] != primary_count[id2]
                               ? primary_count[id1] < primary_count[id2]
                               : id1 < id2;
                });
                int i = 0, j = t_alive_nodes - 1;
                while (i < j && primary_count[node_id[j]] - primary_count[node_id[i]] == 2) {
                    const node_state &ns_max = (*t_global_view->nodes)[address_vec[node_id[j]]];
                    const node_state &ns_min = (*t_global_view->nodes)[address_vec[node_id[i]]];
                    ns_max.for_each_primary(app->app_id, [&](const dsn::gpid &pid) {
                        const partition_configuration &pc =
                            app->partitions[pid.get_partition_index()];
                        dassert(!is_member(pc, ns_min.addr()),
                                "should be moved from(%s) to(%s) gpid(%d.%d)",
                                ns_max.addr().to_string(),
                                ns_min.addr().to_string(),
                                pid.get_app_id(),
                                pid.get_partition_index());
                        t_migration_result->emplace(
                            pid,
                            generate_balancer_request(
                                pc, balance_type::copy_primary, ns_max.addr(), ns_min.addr()));
                        return false;
                    });
                    ++i;
                    --j;
                }
            }
            return false;
        });
    return true;
}

bool greedy_load_balancer::all_replica_infos_collected(const node_state &ns)
{
    dsn::rpc_address n = ns.addr();
    return ns.for_each_partition([this, n](const dsn::gpid &pid) {
        config_context &cc = *get_config_context(*(t_global_view->apps), pid);
        if (cc.find_from_serving(n) == cc.serving.end()) {
            ddebug("meta server hasn't collected gpid(%d.%d)'s info of %s",
                   pid.get_app_id(),
                   pid.get_partition_index(),
                   n.to_string());
            return false;
        }
        return true;
    });
}

void greedy_load_balancer::greedy_balancer(const bool balance_checker)
{
    const app_mapper &apps = *t_global_view->apps;

    dassert(t_alive_nodes > 2, "too few nodes will be freezed");
    number_nodes(*t_global_view->nodes);

    for (auto &kv : *(t_global_view->nodes)) {
        node_state &ns = kv.second;
        if (!all_replica_infos_collected(ns)) {
            return;
        }
    }

    for (const auto &kv : apps) {
        const std::shared_ptr<app_state> &app = kv.second;
        if (app->status != app_status::AS_AVAILABLE)
            continue;

        bool enough_information = primary_balancer_per_app(app);
        if (!enough_information) {
            // Even if we don't have enough info for current app,
            // the decisions made by previous apps are kept.
            // t_migration_result->empty();
            return;
        }
        if (!balance_checker) {
            if (!t_migration_result->empty()) {
                if (_balancer_in_turn) {
                    ddebug("stop to handle more apps after we found some actions for %s",
                           app->get_logname());
                    return;
                }
            }
        }
    }

    // TODO: do primary_balancer_globally when we find a good approach to
    // make decision according to disk load.
    // primary_balancer_globally();

    if (!balance_checker) {
        if (!t_migration_result->empty()) {
            ddebug("stop to do secondary balance coz we already has actions to do");
            return;
        }
    }
    if (_only_primary_balancer) {
        ddebug("stop to do secondary balancer coz it is not allowed");
        return;
    }

    // we seperate the primary/secondary balancer for 2 reasons:
    // 1. globally primary balancer may make secondary unbalanced
    // 2. in one-by-one mode, a secondary balance decision for an app may be prior than
    // another app's primary balancer if not seperated.
    for (const auto &kv : apps) {
        const std::shared_ptr<app_state> &app = kv.second;
        if (app->status != app_status::AS_AVAILABLE)
            continue;

        bool enough_information = copy_secondary_per_app(app);
        if (!enough_information) {
            // Even if we don't have enough info for current app,
            // the decisions made by previous apps are kept.
            // t_migration_result->empty();
            return;
        }
        if (!balance_checker) {
            if (!t_migration_result->empty()) {
                if (_balancer_in_turn) {
                    ddebug("stop to handle more apps after we found some actions for %s",
                           app->get_logname());
                    return;
                }
            }
        }
    }
}

bool greedy_load_balancer::balance(meta_view view, migration_list &list)
{
    ddebug("balancer round");
    list.clear();

    t_total_partitions = count_partitions(*(view.apps));
    t_alive_nodes = view.nodes->size();
    t_global_view = &view;
    t_migration_result = &list;
    t_migration_result->clear();

    greedy_balancer(false);
    return !t_migration_result->empty();
}

bool greedy_load_balancer::check(meta_view view, migration_list &list)
{
    ddebug("balance checker round");
    list.clear();

    t_total_partitions = count_partitions(*(view.apps));
    t_alive_nodes = view.nodes->size();
    t_global_view = &view;
    t_migration_result = &list;
    t_migration_result->clear();

    greedy_balancer(true);
    return !t_migration_result->empty();
}

void greedy_load_balancer::report(const dsn::replication::migration_list &list,
                                  bool balance_checker)
{
    int counters[MAX_COUNT];
    ::memset(counters, 0, sizeof(counters));

    counters[ALL_COUNT] = list.size();
    for (const auto &action : list) {
        switch (action.second.get()->balance_type) {
        case balancer_request_type::move_primary:
            counters[MOVE_PRI_COUNT]++;
            break;
        case balancer_request_type::copy_primary:
            counters[COPY_PRI_COUNT]++;
            break;
        case balancer_request_type::copy_secondary:
            counters[COPY_SEC_COUNT]++;
            break;
        default:
            dassert(false, "");
        }
    }
    ::memcpy(t_operation_counters, counters, sizeof(counters));

    // update perf counters
    _balance_operation_count->set(list.size());
    if (!balance_checker) {
        _recent_balance_move_primary_count->add(counters[MOVE_PRI_COUNT]);
        _recent_balance_copy_primary_count->add(counters[COPY_PRI_COUNT]);
        _recent_balance_copy_secondary_count->add(counters[COPY_SEC_COUNT]);
    }
}
}
}
