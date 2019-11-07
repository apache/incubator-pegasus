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

namespace pegasus {
namespace server {

class result_writer;
static const int HOTSPOT_MAX_MIN_RATIO_THRESHOLD = 10;

class info_collector
{
public:
    struct AppStatCounters
    {
        void set(const row_data &row)
        {
            get_qps->set(row.get_qps);
            multi_get_qps->set(row.multi_get_qps);
            put_qps->set(row.put_qps);
            multi_put_qps->set(row.multi_put_qps);
            remove_qps->set(row.remove_qps);
            multi_remove_qps->set(row.multi_remove_qps);
            incr_qps->set(row.incr_qps);
            check_and_set_qps->set(row.check_and_set_qps);
            check_and_mutate_qps->set(row.check_and_mutate_qps);
            scan_qps->set(row.scan_qps);
            recent_read_cu->set(row.recent_read_cu);
            recent_write_cu->set(row.recent_write_cu);
            recent_expire_count->set(row.recent_expire_count);
            recent_filter_count->set(row.recent_filter_count);
            recent_abnormal_count->set(row.recent_abnormal_count);
            recent_write_throttling_delay_count->set(row.recent_write_throttling_delay_count);
            recent_write_throttling_reject_count->set(row.recent_write_throttling_reject_count);
            storage_mb->set(row.storage_mb);
            storage_count->set(row.storage_count);
            rdb_block_cache_hit_rate->set(std::abs(row.rdb_block_cache_total_count) < 1e-6
                                              ? 0
                                              : row.rdb_block_cache_hit_count /
                                                    row.rdb_block_cache_total_count * 1000000);
            rdb_index_and_filter_blocks_mem_usage->set(row.rdb_index_and_filter_blocks_mem_usage);
            rdb_memtable_mem_usage->set(row.rdb_memtable_mem_usage);
            read_qps->set(row.get_read_qps());
            write_qps->set(row.get_write_qps());

            double qps_ratio = row.max_qps / row.min_qps;
            double cu_ratio = row.max_cu / row.min_cu;
            qps_max_min_ratio->set(qps_ratio);
            cu_max_min_ratio->set(cu_ratio);
            if (qps_ratio >= HOTSPOT_MAX_MIN_RATIO_THRESHOLD) {
                ddebug("the ratio of max/min is larger than 10 for qps(partition id: %s)",
                       row.max_qps_partition_id.c_str());
            }
            if (cu_ratio >= HOTSPOT_MAX_MIN_RATIO_THRESHOLD) {
                ddebug("the ratio of max/min is larger than 10 for cu(partition id: %s)",
                       row.max_cu_partition_id.c_str());
            }
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
        ::dsn::perf_counter_wrapper rdb_index_and_filter_blocks_mem_usage;
        ::dsn::perf_counter_wrapper rdb_memtable_mem_usage;
        ::dsn::perf_counter_wrapper read_qps;
        ::dsn::perf_counter_wrapper write_qps;
        ::dsn::perf_counter_wrapper qps_max_min_ratio;
        ::dsn::perf_counter_wrapper cu_max_min_ratio;
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
};
} // namespace server
} // namespace pegasus
