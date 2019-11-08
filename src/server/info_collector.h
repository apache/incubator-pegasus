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
static const int HOTSPOT_MAX_MIN_SCALE_THRESHOLD = 10;

class info_collector
{
public:
    struct row_statistics
    {
        row_statistics(const std::string &app_name) : app_name(app_name) {}

        double get_read_qps() const { return get_qps + multi_get_qps + scan_qps; }

        double get_write_qps() const
        {
            return put_qps + multi_put_qps + remove_qps + multi_remove_qps + incr_qps +
                   check_and_set_qps + check_and_mutate_qps;
        }

        void calc(const row_data &row)
        {
            get_qps += row.get_qps;
            multi_get_qps += row.multi_get_qps;
            put_qps += row.put_qps;
            multi_put_qps += row.multi_put_qps;
            remove_qps += row.remove_qps;
            multi_remove_qps += row.multi_remove_qps;
            incr_qps += row.incr_qps;
            check_and_set_qps += row.check_and_set_qps;
            check_and_mutate_qps += row.check_and_mutate_qps;
            scan_qps += row.scan_qps;
            recent_read_cu += row.recent_read_cu;
            recent_write_cu += row.recent_write_cu;
            recent_expire_count += row.recent_expire_count;
            recent_filter_count += row.recent_filter_count;
            recent_abnormal_count += row.recent_abnormal_count;
            recent_write_throttling_delay_count += row.recent_write_throttling_delay_count;
            recent_write_throttling_reject_count += row.recent_write_throttling_reject_count;
            storage_mb += row.storage_mb;
            storage_count += row.storage_count;
            rdb_block_cache_hit_count += row.rdb_block_cache_hit_count;
            rdb_block_cache_total_count += row.rdb_block_cache_total_count;
            rdb_index_and_filter_blocks_mem_usage += row.rdb_index_and_filter_blocks_mem_usage;
            rdb_memtable_mem_usage += row.rdb_memtable_mem_usage;

            // get max_total_qps、min_total_qps and the id of this partition which has max_total_qps
            double row_total_qps = row.get_total_qps();
            if (max_total_qps < row_total_qps) {
                max_total_qps = row_total_qps;
                max_qps_partition_id = row.row_name;
            } else if (min_total_qps > row_total_qps) {
                min_total_qps = row_total_qps;
            }

            // get max_total_cu、min_total_cu and the id of this partition which has max_total_cu
            double row_total_cu = row.get_total_cu();
            if (max_total_cu < row_total_cu) {
                max_total_cu = row_total_cu;
                max_cu_partition_id = row.row_name;
            } else if (min_total_cu > row_total_cu) {
                min_total_cu = row_total_cu;
            }
        }

        void merge(const row_statistics &row_stats)
        {
            get_qps += row_stats.get_qps;
            multi_get_qps += row_stats.multi_get_qps;
            put_qps += row_stats.put_qps;
            multi_put_qps += row_stats.multi_put_qps;
            remove_qps += row_stats.remove_qps;
            multi_remove_qps += row_stats.multi_remove_qps;
            incr_qps += row_stats.incr_qps;
            check_and_set_qps += row_stats.check_and_set_qps;
            check_and_mutate_qps += row_stats.check_and_mutate_qps;
            scan_qps += row_stats.scan_qps;
            recent_read_cu += row_stats.recent_read_cu;
            recent_write_cu += row_stats.recent_write_cu;
            recent_expire_count += row_stats.recent_expire_count;
            recent_filter_count += row_stats.recent_filter_count;
            recent_abnormal_count += row_stats.recent_abnormal_count;
            recent_write_throttling_delay_count += row_stats.recent_write_throttling_delay_count;
            recent_write_throttling_reject_count += row_stats.recent_write_throttling_reject_count;
            storage_mb += row_stats.storage_mb;
            storage_count += row_stats.storage_count;
            rdb_block_cache_hit_count += row_stats.rdb_block_cache_hit_count;
            rdb_block_cache_total_count += row_stats.rdb_block_cache_total_count;
            rdb_index_and_filter_blocks_mem_usage +=
                row_stats.rdb_index_and_filter_blocks_mem_usage;
            rdb_memtable_mem_usage += row_stats.rdb_memtable_mem_usage;

            // We only need max_total_qps/min_total_qps/max_total_cu/min_total_cu in the same app
            if (this->app_name == row_stats.app_name) {
                // get max_total_qps、min_total_qps and the id of this partition which has
                // max_total_qps
                if (max_total_qps < row_stats.max_total_qps) {
                    max_total_qps = row_stats.max_total_qps;
                    max_qps_partition_id = row_stats.max_qps_partition_id;
                }
                min_total_qps = std::min(min_total_qps, row_stats.min_total_qps);

                // get max_total_cu、min_total_cu and the id of this partition which has
                // max_total_cu
                if (max_total_cu < row_stats.max_total_cu) {
                    max_total_cu = row_stats.max_total_cu;
                    max_cu_partition_id = row_stats.max_cu_partition_id;
                }
                min_total_cu = std::min(min_total_cu, row_stats.min_total_cu);
            }
        }

        std::string app_name;
        double get_qps = 0;
        double multi_get_qps = 0;
        double put_qps = 0;
        double multi_put_qps = 0;
        double remove_qps = 0;
        double multi_remove_qps = 0;
        double incr_qps = 0;
        double check_and_set_qps = 0;
        double check_and_mutate_qps = 0;
        double scan_qps = 0;
        double recent_read_cu = 0;
        double recent_write_cu = 0;
        double recent_expire_count = 0;
        double recent_filter_count = 0;
        double recent_abnormal_count = 0;
        double recent_write_throttling_delay_count = 0;
        double recent_write_throttling_reject_count = 0;
        double storage_mb = 0;
        double storage_count = 0;
        double rdb_block_cache_hit_count = 0;
        double rdb_block_cache_total_count = 0;
        double rdb_index_and_filter_blocks_mem_usage = 0;
        double rdb_memtable_mem_usage = 0;

        // used when merging
        double max_total_qps = 0;
        double min_total_qps = INT_MAX;
        double max_total_cu = 0;
        double min_total_cu = INT_MAX;
        std::string max_qps_partition_id = 0;
        std::string max_cu_partition_id = 0;
    };

    struct AppStatCounters
    {
        void set(const row_statistics &row_stats)
        {
            get_qps->set(row_stats.get_qps);
            multi_get_qps->set(row_stats.multi_get_qps);
            put_qps->set(row_stats.put_qps);
            multi_put_qps->set(row_stats.multi_put_qps);
            remove_qps->set(row_stats.remove_qps);
            multi_remove_qps->set(row_stats.multi_remove_qps);
            incr_qps->set(row_stats.incr_qps);
            check_and_set_qps->set(row_stats.check_and_set_qps);
            check_and_mutate_qps->set(row_stats.check_and_mutate_qps);
            scan_qps->set(row_stats.scan_qps);
            recent_read_cu->set(row_stats.recent_read_cu);
            recent_write_cu->set(row_stats.recent_write_cu);
            recent_expire_count->set(row_stats.recent_expire_count);
            recent_filter_count->set(row_stats.recent_filter_count);
            recent_abnormal_count->set(row_stats.recent_abnormal_count);
            recent_write_throttling_delay_count->set(row_stats.recent_write_throttling_delay_count);
            recent_write_throttling_reject_count->set(
                row_stats.recent_write_throttling_reject_count);
            storage_mb->set(row_stats.storage_mb);
            storage_count->set(row_stats.storage_count);
            rdb_block_cache_hit_rate->set(std::abs(row_stats.rdb_block_cache_total_count) < 1e-6
                                              ? 0
                                              : row_stats.rdb_block_cache_hit_count /
                                                    row_stats.rdb_block_cache_total_count *
                                                    1000000);
            rdb_index_and_filter_blocks_mem_usage->set(
                row_stats.rdb_index_and_filter_blocks_mem_usage);
            rdb_memtable_mem_usage->set(row_stats.rdb_memtable_mem_usage);
            read_qps->set(row_stats.get_read_qps());
            write_qps->set(row_stats.get_write_qps());

            double qps_scale = row_stats.max_total_qps / std::max(row_stats.min_total_qps, 1.0);
            double cu_scale = row_stats.max_total_cu / std::max(row_stats.min_total_cu, 1.0);
            qps_max_min_scale->set(qps_scale);
            cu_max_min_scale->set(cu_scale);
            if (qps_scale >= HOTSPOT_MAX_MIN_SCALE_THRESHOLD) {
                ddebug("There is a hot spot about qps in %s(%s))",
                       row_stats.app_name.c_str(),
                       row_stats.max_qps_partition_id.c_str());
            }
            if (cu_scale >= HOTSPOT_MAX_MIN_SCALE_THRESHOLD) {
                ddebug("There is a hot spot about cu in %s(%s))",
                       row_stats.app_name.c_str(),
                       row_stats.max_cu_partition_id.c_str());
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
        ::dsn::perf_counter_wrapper qps_max_min_scale;
        ::dsn::perf_counter_wrapper cu_max_min_scale;
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
