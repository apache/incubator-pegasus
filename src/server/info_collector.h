// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/tool-api/task_tracker.h>
#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>

#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <evhttp.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/bufferevent.h>
#include <fstream>

#include "../shell/commands.h"
#include "table_stats.h"
#include "table_hotspot_policy.h"

namespace pegasus {
namespace server {

class result_writer;
static const int HOTSPOT_MAX_MIN_SCALE_THRESHOLD = 10;

class info_collector
{
public:
    struct AppStatCounters
    {
        void set(const table_stats &row_stats)
        {
            get_qps->set(row_stats.total_get_qps);
            multi_get_qps->set(row_stats.total_multi_get_qps);
            put_qps->set(row_stats.total_put_qps);
            multi_put_qps->set(row_stats.total_multi_put_qps);
            remove_qps->set(row_stats.total_remove_qps);
            multi_remove_qps->set(row_stats.total_multi_remove_qps);
            incr_qps->set(row_stats.total_incr_qps);
            check_and_set_qps->set(row_stats.total_check_and_set_qps);
            check_and_mutate_qps->set(row_stats.total_check_and_mutate_qps);
            scan_qps->set(row_stats.total_scan_qps);
            recent_read_cu->set(row_stats.total_recent_read_cu);
            recent_write_cu->set(row_stats.total_recent_write_cu);
            recent_expire_count->set(row_stats.total_recent_expire_count);
            recent_filter_count->set(row_stats.total_recent_filter_count);
            recent_abnormal_count->set(row_stats.total_recent_abnormal_count);
            recent_write_throttling_delay_count->set(
                row_stats.total_recent_write_throttling_delay_count);
            recent_write_throttling_reject_count->set(
                row_stats.total_recent_write_throttling_reject_count);
            storage_mb->set(row_stats.total_storage_mb);
            storage_count->set(row_stats.total_storage_count);
            rdb_block_cache_hit_rate->set(
                std::abs(row_stats.total_rdb_block_cache_total_count) < 1e-6
                    ? 0
                    : row_stats.total_rdb_block_cache_hit_count /
                          row_stats.total_rdb_block_cache_total_count * 1000000);
            rdb_index_and_filter_blocks_mem_usage->set(
                row_stats.total_rdb_index_and_filter_blocks_mem_usage);
            rdb_memtable_mem_usage->set(row_stats.total_rdb_memtable_mem_usage);
            read_qps->set(row_stats.get_total_read_qps());
            write_qps->set(row_stats.get_total_write_qps());
        }

        ::dsn::perf_counter_wrapper get_qps;
        ::dsn::perf_counter_wrapper multi_get_qps;
        ::dsn::perf_counter_wrapper put_qps;
        ::dsn::perf_counter_wrapper multi_put_qps;
        ::dsn::perf_counter_wrapper remove_qps;
        ::dsn::perf_counter_wrapper multi_remove_qps;
        ::dsn::perf_counter_wrapper incr_qps;
        ::dsn::perf_counter_wrapper check_and_set_qps;
        ::dsn::perf_counter_wrapper check_and_mutate_qps;
        ::dsn::perf_counter_wrapper scan_qps;
        ::dsn::perf_counter_wrapper recent_read_cu;
        ::dsn::perf_counter_wrapper recent_write_cu;
        ::dsn::perf_counter_wrapper recent_expire_count;
        ::dsn::perf_counter_wrapper recent_filter_count;
        ::dsn::perf_counter_wrapper recent_abnormal_count;
        ::dsn::perf_counter_wrapper recent_write_throttling_delay_count;
        ::dsn::perf_counter_wrapper recent_write_throttling_reject_count;
        ::dsn::perf_counter_wrapper storage_mb;
        ::dsn::perf_counter_wrapper storage_count;
        ::dsn::perf_counter_wrapper rdb_block_cache_hit_rate;
        ::dsn::perf_counter_wrapper rdb_block_cache_mem_usage;
        ::dsn::perf_counter_wrapper rdb_index_and_filter_blocks_mem_usage;
        ::dsn::perf_counter_wrapper rdb_memtable_mem_usage;
        ::dsn::perf_counter_wrapper rdb_estimate_num_keys;
        ::dsn::perf_counter_wrapper read_qps;
        ::dsn::perf_counter_wrapper write_qps;
        ::dsn::perf_counter_wrapper qps_skew;
        ::dsn::perf_counter_wrapper cu_skew;
    };

    info_collector();
    ~info_collector();

    void start();
    void stop();

    void on_app_stat();
    AppStatCounters *get_app_counters(const std::string &app_name);

    void on_capacity_unit_stat(int remaining_retry_count);
    bool has_capacity_unit_updated(const std::string &node_address, const std::string &timestamp);

    void on_storage_size_stat(int remaining_retry_count);

private:
    dsn::task_tracker _tracker;
    ::dsn::rpc_address _meta_servers;
    std::string _cluster_name;
    shell_context _shell_context;
    uint32_t _app_stat_interval_seconds;
    ::dsn::task_ptr _app_stat_timer_task;
    ::dsn::utils::ex_lock_nr _app_stat_counter_lock;
    std::map<std::string, AppStatCounters *> _app_stat_counters;

    // app for recording usage statistics, including read/write capacity unit and storage size.
    std::string _usage_stat_app;
    // client to access server.
    pegasus_client *_client;
    // for writing cu stat result
    std::unique_ptr<result_writer> _result_writer;
    uint32_t _capacity_unit_fetch_interval_seconds;
    uint32_t _capacity_unit_retry_wait_seconds;
    uint32_t _capacity_unit_retry_max_count;
    ::dsn::task_ptr _capacity_unit_stat_timer_task;
    uint32_t _storage_size_fetch_interval_seconds;
    uint32_t _storage_size_retry_wait_seconds;
    uint32_t _storage_size_retry_max_count;
    ::dsn::task_ptr _storage_size_stat_timer_task;
    ::dsn::utils::ex_lock_nr _capacity_unit_update_info_lock;
    // mapping 'node address' --> 'last updated timestamp'
    std::map<std::string, string> _capacity_unit_update_info;
    std::map<std::string, hotspot_calculator *> _calculator_store;

    hotspot_calculator *get_store_handler(const std::string app_name, const int partition_num){}
};
} // namespace server
} // namespace pegasus
