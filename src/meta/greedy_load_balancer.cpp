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

#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <string.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "app_balance_policy.h"
#include "cluster_balance_policy.h"
#include "greedy_load_balancer.h"
#include "meta/load_balance_policy.h"
#include "meta/meta_service.h"
#include "meta/server_load_balancer.h"
#include "meta/server_state.h"
#include "meta/table_metrics.h"
#include "meta_admin_types.h"
#include "meta_data.h"
#include "runtime/rpc/rpc_host_port.h"
#include "utils/command_manager.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/math.h"
#include "utils/metrics.h"

DSN_DEFINE_bool(meta_server, balance_cluster, false, "whether to enable cluster balancer");
DSN_TAG_VARIABLE(balance_cluster, FT_MUTABLE);

DSN_DECLARE_uint64(min_live_node_count_for_unfreeze);

namespace dsn {
class gpid;

namespace replication {

greedy_load_balancer::greedy_load_balancer(meta_service *_svc) : server_load_balancer(_svc)
{
    _app_balance_policy = std::make_unique<app_balance_policy>(_svc);
    _cluster_balance_policy = std::make_unique<cluster_balance_policy>(_svc);
    _all_replca_infos_collected = false;

    ::memset(t_operation_counters, 0, sizeof(t_operation_counters));
}

greedy_load_balancer::~greedy_load_balancer() {}

void greedy_load_balancer::register_ctrl_commands()
{
    _get_balance_operation_count = dsn::command_manager::instance().register_single_command(
        "meta.lb.get_balance_operation_count",
        "Get balance operation count",
        "[total | move_pri | copy_pri | copy_sec | detail]",
        [this](const std::vector<std::string> &args) { return get_balance_operation_count(args); });
}

std::string greedy_load_balancer::get_balance_operation_count(const std::vector<std::string> &args)
{
    nlohmann::json info;
    if (args.size() > 1) {
        info["error"] = fmt::format("invalid arguments");
    } else if (args.empty() || args[0] == "total") {
        info["total"] = t_operation_counters[ALL_COUNT];
    } else if (args[0] == "move_pri") {
        info["move_pri"] = t_operation_counters[MOVE_PRI_COUNT];
    } else if (args[0] == "copy_pri") {
        info["copy_pri"] = t_operation_counters[COPY_PRI_COUNT];
    } else if (args[0] == "copy_sec") {
        info["copy_sec"] = t_operation_counters[COPY_SEC_COUNT];
    } else if (args[0] == "detail") {
        info["move_pri"] = t_operation_counters[MOVE_PRI_COUNT];
        info["copy_pri"] = t_operation_counters[COPY_PRI_COUNT];
        info["copy_sec"] = t_operation_counters[COPY_SEC_COUNT];
        info["total"] = t_operation_counters[ALL_COUNT];
    } else {
        info["error"] = fmt::format("invalid arguments");
    }
    return info.dump(2);
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

bool greedy_load_balancer::all_replica_infos_collected(const node_state &ns)
{
    const auto &n = ns.host_port();
    return ns.for_each_partition([this, n](const dsn::gpid &pid) {
        config_context &cc = *get_config_context(*(t_global_view->apps), pid);
        if (cc.find_from_serving(n) == cc.serving.end()) {
            LOG_INFO("meta server hasn't collected gpid({})'s info of {}", pid, n);
            return false;
        }
        return true;
    });
}

void greedy_load_balancer::greedy_balancer(const bool balance_checker)
{
    CHECK_GE_MSG(
        t_alive_nodes, FLAGS_min_live_node_count_for_unfreeze, "too few nodes will be freezed");

    for (auto &kv : *(t_global_view->nodes)) {
        node_state &ns = kv.second;
        _all_replca_infos_collected = all_replica_infos_collected(ns);
        if (!_all_replca_infos_collected) {
            return;
        }
    }

    load_balance_policy *balance_policy = nullptr;
    if (!FLAGS_balance_cluster) {
        balance_policy = _app_balance_policy.get();
    } else if (!balance_checker) {
        balance_policy = _cluster_balance_policy.get();
    }
    if (balance_policy != nullptr) {
        balance_policy->balance(balance_checker, t_global_view, t_migration_result);
    }
}

bool greedy_load_balancer::balance(meta_view view, migration_list &list)
{
    LOG_INFO("balancer round");
    list.clear();

    t_alive_nodes = view.nodes->size();
    t_global_view = &view;
    t_migration_result = &list;
    t_migration_result->clear();

    greedy_balancer(false);
    return !t_migration_result->empty();
}

bool greedy_load_balancer::check(meta_view view, migration_list &list)
{
    LOG_INFO("balance checker round");
    list.clear();

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
#define __METRIC_INCREMENT(name)                                                                   \
    METRIC_INCREMENT(balance_stats, name, action.first, balance_checker)

    int counters[MAX_COUNT] = {0};
    greedy_balance_stats balance_stats;

    counters[ALL_COUNT] = list.size();
    for (const auto &action : list) {
        switch (action.second.get()->balance_type) {
        case balancer_request_type::move_primary:
            counters[MOVE_PRI_COUNT]++;
            __METRIC_INCREMENT(greedy_move_primary_operations);
            break;
        case balancer_request_type::copy_primary:
            counters[COPY_PRI_COUNT]++;
            __METRIC_INCREMENT(greedy_copy_primary_operations);
            break;
        case balancer_request_type::copy_secondary:
            counters[COPY_SEC_COUNT]++;
            __METRIC_INCREMENT(greedy_copy_secondary_operations);
            break;
        default:
            CHECK(false, "");
        }
    }

    if (!_all_replca_infos_collected) {
        counters[ALL_COUNT] = -1;
        LOG_DEBUG(
            "balance checker operation count = {}, due to meta server hasn't collected all replica",
            counters[ALL_COUNT]);
    }

    ::memcpy(t_operation_counters, counters, sizeof(counters));
    METRIC_SET_GREEDY_BALANCE_STATS(_svc->get_server_state()->get_table_metric_entities(),
                                    balance_stats);

#undef __METRIC_INCREMENT
}
} // namespace replication
} // namespace dsn
