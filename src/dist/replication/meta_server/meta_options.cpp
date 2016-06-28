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

namespace dsn { namespace replication {

std::string meta_options::concat_path_unix_style(const std::string &prefix, const std::string &postfix)
{
    size_t pos1 = prefix.size(); // last_valid_pos + 1
    while (pos1 > 0 && prefix[pos1-1] == '/') pos1--;
    size_t pos2 = 0; // first non '/' position
    while (pos2 < postfix.size() && postfix[pos2] == '/') pos2++;
    return prefix.substr(0, pos1) + "/" + postfix.substr(pos2);
}

void meta_options::initialize()
{
    cluster_root = dsn_config_get_value_string("meta_server", "cluster_root", "/", "cluster root of meta state service on remote");

    distributed_lock_service_type = dsn_config_get_value_string(
        "meta_server",
        "distributed_lock_service_type",
        "distributed_lock_service_simple",
        "dist lock provider");
    meta_state_service_type = dsn_config_get_value_string(
        "meta_server",
        "meta_state_service_type",
        "meta_state_service_simple",
        "meta_state_service provider type"
        );
    server_load_balancer_type = dsn_config_get_value_string(
        "meta_server",
        "server_load_balancer_type",
        "simple_load_balancer",
        "server load balancer provider"
        );

    const char* meta_state_service_parameters = dsn_config_get_value_string(
        "meta_server",
        "meta_state_service_parameters",
        "",
        "meta_state_service provider parameters"
        );
    utils::split_args(meta_state_service_parameters, meta_state_service_args);

    const char* distributed_lock_service_parameters = dsn_config_get_value_string(
        "meta_server",
        "distributed_lock_service_parameters",
        "",
        "distributed_lock_service provider parameters"
        );
    utils::split_args(distributed_lock_service_parameters, distributed_lock_service_args);

    replica_assign_delay_ms_for_dropouts = dsn_config_get_value_uint64(
        "meta_server",
        "replica_assign_delay_ms_for_dropouts",
        300000,
        "replica_assign_delay_ms_for_dropouts, default is 300000");

    node_live_percentage_threshold_for_update = dsn_config_get_value_uint64(
        "meta_server",
        "node_live_percentage_threshold_for_update",
        50,
        "if live_node_count * 100 < total_node_count * node_live_percentage_threshold_for_update, then freeze the cluster; default is 50");

    min_live_node_count_for_unfreeze = dsn_config_get_value_uint64(
        "meta_server",
        "min_live_node_count_for_unfreeze",
        3,
        "minimum live node count without which the state is freezed"
        );
}

}}
