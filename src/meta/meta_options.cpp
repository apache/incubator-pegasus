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
 *     the meta server's options, impl file
 *
 * Revision history:
 *     2016-04-25, Weijie Sun(sunweijie at xiaomi.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#include "meta_options.h"

#include "utils/flags.h"

namespace dsn {
namespace replication {

std::string meta_options::concat_path_unix_style(const std::string &prefix,
                                                 const std::string &postfix)
{
    size_t pos1 = prefix.size(); // last_valid_pos + 1
    while (pos1 > 0 && prefix[pos1 - 1] == '/')
        pos1--;
    size_t pos2 = 0; // first non '/' position
    while (pos2 < postfix.size() && postfix[pos2] == '/')
        pos2++;
    return prefix.substr(0, pos1) + "/" + postfix.substr(pos2);
}

void meta_options::initialize()
{
    cluster_root = dsn_config_get_value_string(
        "meta_server", "cluster_root", "/", "cluster root of meta state service on remote");

    meta_state_service_type = dsn_config_get_value_string("meta_server",
                                                          "meta_state_service_type",
                                                          "meta_state_service_simple",
                                                          "meta_state_service provider type");
    const char *meta_state_service_parameters =
        dsn_config_get_value_string("meta_server",
                                    "meta_state_service_parameters",
                                    "",
                                    "meta_state_service provider parameters");
    utils::split_args(meta_state_service_parameters, meta_state_service_args);

    node_live_percentage_threshold_for_update = dsn_config_get_value_uint64(
        "meta_server",
        "node_live_percentage_threshold_for_update",
        65,
        "if live_node_count * 100 < total_node_count * node_live_percentage_threshold_for_update, "
        "then freeze the cluster; default is 65");

    meta_function_level_on_start = meta_function_level::fl_invalid;
    const char *level_str = dsn_config_get_value_string(
        "meta_server", "meta_function_level_on_start", "steady", "meta function level on start");
    std::string level = std::string("fl_") + level_str;
    for (auto &kv : _meta_function_level_VALUES_TO_NAMES) {
        if (level == kv.second) {
            meta_function_level_on_start = (meta_function_level::type)kv.first;
            break;
        }
    }
    CHECK_NE_MSG(meta_function_level_on_start,
                 meta_function_level::fl_invalid,
                 "invalid function level: {}",
                 level_str);

    recover_from_replica_server = dsn_config_get_value_bool(
        "meta_server",
        "recover_from_replica_server",
        false,
        "whether to recover from replica server when no apps in remote storage");

    hold_seconds_for_dropped_app =
        dsn_config_get_value_uint64("meta_server",
                                    "hold_seconds_for_dropped_app",
                                    604800,
                                    "how long to hold data for dropped apps");

    add_secondary_enable_flow_control =
        dsn_config_get_value_bool("meta_server",
                                  "add_secondary_enable_flow_control",
                                  false,
                                  "enable flow control for add secondary proposal");
    add_secondary_max_count_for_one_node = dsn_config_get_value_uint64(
        "meta_server",
        "add_secondary_max_count_for_one_node",
        10,
        "add secondary max count for one node when flow control enabled");

    /// failure detector options
    _fd_opts.distributed_lock_service_type =
        dsn_config_get_value_string("meta_server",
                                    "distributed_lock_service_type",
                                    "distributed_lock_service_simple",
                                    "dist lock provider");
    const char *distributed_lock_service_parameters =
        dsn_config_get_value_string("meta_server",
                                    "distributed_lock_service_parameters",
                                    "",
                                    "distributed_lock_service provider parameters");
    utils::split_args(distributed_lock_service_parameters, _fd_opts.distributed_lock_service_args);
    _fd_opts.stable_rs_min_running_seconds =
        dsn_config_get_value_uint64("meta_server",
                                    "stable_rs_min_running_seconds",
                                    600,
                                    "min running seconds for a stable replica server");

    _fd_opts.max_succssive_unstable_restart = dsn_config_get_value_uint64(
        "meta_server",
        "max_succssive_unstable_restart",
        5,
        "meta server will treat an rs unstable so as to reject it's beacons "
        "if its succssively restarting count exceeds this value");

    /// load balancer options
    _lb_opts.server_load_balancer_type =
        dsn_config_get_value_string("meta_server",
                                    "server_load_balancer_type",
                                    "greedy_load_balancer",
                                    "server load balancer provider");
    _lb_opts.replica_assign_delay_ms_for_dropouts =
        dsn_config_get_value_uint64("meta_server",
                                    "replica_assign_delay_ms_for_dropouts",
                                    300000,
                                    "replica_assign_delay_ms_for_dropouts, default is 300000");
    _lb_opts.max_replicas_in_group = dsn_config_get_value_uint64(
        "meta_server", "max_replicas_in_group", 4, "max replicas(alive & dead) in a group");

    _lb_opts.balancer_in_turn = dsn_config_get_value_bool(
        "meta_server", "balancer_in_turn", false, "balance the apps one-by-one/concurrently");
    _lb_opts.only_primary_balancer = dsn_config_get_value_bool(
        "meta_server", "only_primary_balancer", false, "only try to make the primary balanced");
    _lb_opts.only_move_primary = dsn_config_get_value_bool(
        "meta_server", "only_move_primary", false, "only try to make the primary balanced by move");

    partition_guardian_type = dsn_config_get_value_string("meta_server",
                                                          "partition_guardian_type",
                                                          "partition_guardian",
                                                          "partition guardian provider");

    cold_backup_disabled = dsn_config_get_value_bool(
        "meta_server", "cold_backup_disabled", true, "whether to disable cold backup");

    enable_white_list =
        dsn_config_get_value_bool("meta_server",
                                  "enable_white_list",
                                  false,
                                  "whether to enable white list of replica servers");

    const char *replica_white_list_raw = dsn_config_get_value_string(
        "meta_server", "replica_white_list", "", "white list of replica-servers in meta-server");
    utils::split_args(replica_white_list_raw, replica_white_list, ',');
}
} // namespace replication
} // namespace dsn
