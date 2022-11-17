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

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"
#include <string>

namespace dsn {
namespace replication {

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

    bool deny_client_on_start;
    bool verbose_client_log_on_start;
    bool verbose_commit_log_on_start;
    bool delay_for_fd_timeout_on_start;
    bool duplication_enabled;

    int32_t prepare_timeout_ms_for_secondaries;
    int32_t prepare_timeout_ms_for_potential_secondaries;
    int32_t prepare_decree_gap_for_debug_logging;

    bool batch_write_disabled;
    int32_t staleness_for_commit;
    int32_t max_mutation_count_in_prepare_list;
    int32_t mutation_2pc_min_replica_count;

    bool group_check_disabled;
    int32_t group_check_interval_ms;

    bool checkpoint_disabled;
    int32_t checkpoint_interval_seconds;
    int64_t checkpoint_min_decree_gap;
    int32_t checkpoint_max_interval_hours;

    bool gc_disabled;
    int32_t gc_interval_ms;
    int32_t gc_memory_replica_interval_ms;

    bool disk_stat_disabled;
    int32_t disk_stat_interval_seconds;

    bool fd_disabled;
    int32_t fd_check_interval_seconds;
    int32_t fd_beacon_interval_seconds;
    int32_t fd_lease_seconds;
    int32_t fd_grace_seconds;

    int32_t log_private_file_size_mb;
    int32_t log_private_reserve_max_size_mb;
    int32_t log_private_reserve_max_time_seconds;

    int32_t log_shared_file_size_mb;
    int32_t log_shared_file_count_limit;
    int32_t log_shared_batch_buffer_kb;
    bool log_shared_force_flush;
    int32_t log_shared_pending_size_throttling_threshold_kb;
    int32_t log_shared_pending_size_throttling_delay_ms;

    bool config_sync_disabled;
    int32_t config_sync_interval_ms;

    bool mem_release_enabled;
    int32_t mem_release_check_interval_ms;
    int32_t mem_release_max_reserved_mem_percentage;

    int32_t lb_interval_ms;

    int32_t learn_app_max_concurrent_count;

    std::string cold_backup_root;
    int32_t cold_backup_checkpoint_reserve_minutes;

    int32_t max_concurrent_bulk_load_downloading_count;

public:
    replication_options();
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

private:
    void sanity_check();
};
} // namespace replication
} // namespace dsn
