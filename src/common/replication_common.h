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

#pragma once

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "metadata_types.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/task/task.h"

namespace dsn {
namespace replication {
class configuration_update_app_env_request;
class configuration_update_app_env_response;
class query_app_info_request;
class query_app_info_response;
class query_replica_info_request;
class query_replica_info_response;

typedef std::unordered_map<::dsn::rpc_address, partition_status::type> node_statuses;
typedef std::unordered_map<::dsn::rpc_address, dsn::task_ptr> node_tasks;

typedef rpc_holder<configuration_update_app_env_request, configuration_update_app_env_response>
    update_app_env_rpc;
typedef rpc_holder<query_app_info_request, query_app_info_response> query_app_info_rpc;
typedef rpc_holder<query_replica_info_request, query_replica_info_response> query_replica_info_rpc;

class replication_options
{
public:
    std::vector<::dsn::rpc_address> meta_servers;

    std::string app_name;
    std::string app_dir;
    std::string slog_dir;
    std::vector<std::string> data_dirs;
    std::vector<std::string> data_dir_tags;

    int32_t max_concurrent_bulk_load_downloading_count;

public:
    replication_options() = default;
    ~replication_options();

    void initialize();
    int32_t app_mutation_2pc_min_replica_count(int32_t app_max_replica_count) const;
    static bool get_data_dir_and_tag(const std::string &config_dirs_str,
                                     const std::string &default_dir,
                                     const std::string &app_name,
                                     /*out*/ std::vector<std::string> &data_dirs,
                                     /*out*/ std::vector<std::string> &data_dir_tags,
                                     /*out*/ std::string &err_msg);
    static void get_data_dirs_in_black_list(const std::string &fname,
                                            /*out*/ std::vector<std::string> &dirs);
    static bool check_if_in_black_list(const std::vector<std::string> &black_list_dir,
                                       const std::string &dir);
};
} // namespace replication
} // namespace dsn
