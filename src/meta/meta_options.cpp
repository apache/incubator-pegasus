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

    /// load balancer options
    _lb_opts.server_load_balancer_type =
        dsn_config_get_value_string("meta_server",
                                    "server_load_balancer_type",
                                    "greedy_load_balancer",
                                    "server load balancer provider");

    partition_guardian_type = dsn_config_get_value_string("meta_server",
                                                          "partition_guardian_type",
                                                          "partition_guardian",
                                                          "partition guardian provider");

    const char *replica_white_list_raw = dsn_config_get_value_string(
        "meta_server", "replica_white_list", "", "white list of replica-servers in meta-server");
    utils::split_args(replica_white_list_raw, replica_white_list, ',');
}
} // namespace replication
} // namespace dsn
