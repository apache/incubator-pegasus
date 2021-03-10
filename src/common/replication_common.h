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

#include <dsn/dist/replication.h>
#include <string>

namespace dsn {
namespace replication {

typedef std::unordered_map<::dsn::rpc_address, partition_status::type> node_statuses;
typedef std::unordered_map<::dsn::rpc_address, dsn::task_ptr> node_tasks;

typedef rpc_holder<configuration_update_app_env_request, configuration_update_app_env_response>
    update_app_env_rpc;

typedef rpc_holder<start_bulk_load_request, start_bulk_load_response> start_bulk_load_rpc;
typedef rpc_holder<bulk_load_request, bulk_load_response> bulk_load_rpc;
typedef rpc_holder<control_bulk_load_request, control_bulk_load_response> control_bulk_load_rpc;
typedef rpc_holder<query_bulk_load_request, query_bulk_load_response> query_bulk_load_rpc;

typedef rpc_holder<start_partition_split_request, start_partition_split_response> start_split_rpc;
typedef rpc_holder<control_split_request, control_split_response> control_split_rpc;
typedef rpc_holder<query_split_request, query_split_response> query_split_rpc;
typedef rpc_holder<register_child_request, register_child_response> register_child_rpc;
typedef rpc_holder<notify_stop_split_request, notify_stop_split_response> notify_stop_split_rpc;
typedef rpc_holder<query_child_state_request, query_child_state_response> query_child_state_rpc;

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
    bool empty_write_disabled;
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
    int32_t log_private_batch_buffer_kb;
    int32_t log_private_batch_buffer_count;
    int32_t log_private_batch_buffer_flush_interval_ms;
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
    void initialize();
    ~replication_options();

private:
    void sanity_check();
};

extern const char *partition_status_to_string(partition_status::type status);

class backup_restore_constant
{
public:
    static const std::string FORCE_RESTORE;
    static const std::string BLOCK_SERVICE_PROVIDER;
    static const std::string CLUSTER_NAME;
    static const std::string POLICY_NAME;
    static const std::string APP_NAME;
    static const std::string APP_ID;
    static const std::string BACKUP_ID;
    static const std::string SKIP_BAD_PARTITION;
};

class bulk_load_constant
{
public:
    static const std::string BULK_LOAD_INFO;
    static const int32_t BULK_LOAD_REQUEST_INTERVAL;
    static const int32_t BULK_LOAD_REQUEST_SHORT_INTERVAL;
    static const std::string BULK_LOAD_METADATA;
    static const std::string BULK_LOAD_LOCAL_ROOT_DIR;
    static const int32_t PROGRESS_FINISHED;
};

} // namespace replication
} // namespace dsn
