// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

struct table_stats
{
    table_stats(const std::string &app_name) : app_name(app_name) {}

    double get_total_read_qps() const
    {
        return total_get_qps + total_multi_get_qps + total_scan_qps;
    }

    double get_total_write_qps() const
    {
        return total_put_qps + total_multi_put_qps + total_remove_qps + total_multi_remove_qps +
               total_incr_qps + total_check_and_set_qps + total_check_and_mutate_qps +
               total_dup_qps;
    }

    double get_total_read_bytes() const
    {
        return total_get_bytes + total_multi_get_bytes + total_scan_bytes;
    }

    double get_total_write_bytes() const
    {
        return total_put_bytes + total_multi_put_bytes + total_check_and_set_bytes +
               total_check_and_mutate_bytes;
    }

    void aggregate(const row_data &row)
    {
        total_get_qps += row.get_qps;
        total_multi_get_qps += row.multi_get_qps;
        total_put_qps += row.put_qps;
        total_multi_put_qps += row.multi_put_qps;
        total_remove_qps += row.remove_qps;
        total_multi_remove_qps += row.multi_remove_qps;
        total_incr_qps += row.incr_qps;
        total_check_and_set_qps += row.check_and_set_qps;
        total_check_and_mutate_qps += row.check_and_mutate_qps;
        total_scan_qps += row.scan_qps;
        total_dup_qps += row.dup_qps;
        total_dup_shipped_ops = row.dup_shipped_ops;
        total_dup_failed_shipping_ops = row.dup_failed_shipping_ops;
        total_recent_read_cu += row.recent_read_cu;
        total_recent_write_cu += row.recent_write_cu;
        total_recent_expire_count += row.recent_expire_count;
        total_recent_filter_count += row.recent_filter_count;
        total_recent_abnormal_count += row.recent_abnormal_count;
        total_recent_write_throttling_delay_count += row.recent_write_throttling_delay_count;
        total_recent_write_throttling_reject_count += row.recent_write_throttling_reject_count;
        total_storage_mb += row.storage_mb;
        total_storage_count += row.storage_count;
        total_rdb_block_cache_hit_count += row.rdb_block_cache_hit_count;
        total_rdb_block_cache_total_count += row.rdb_block_cache_total_count;
        total_rdb_index_and_filter_blocks_mem_usage += row.rdb_index_and_filter_blocks_mem_usage;
        total_rdb_memtable_mem_usage += row.rdb_memtable_mem_usage;
        total_rdb_estimate_num_keys += row.rdb_estimate_num_keys;
        total_backup_request_qps += row.backup_request_qps;
        total_get_bytes += row.get_bytes;
        total_multi_get_bytes += row.multi_get_bytes;
        total_scan_bytes += row.scan_bytes;
        total_put_bytes += row.put_bytes;
        total_multi_put_bytes += row.multi_put_bytes;
        total_check_and_set_bytes += row.check_and_set_bytes;
        total_check_and_mutate_bytes += row.check_and_mutate_bytes;
    }

    void merge(const table_stats &row_stats)
    {
        total_get_qps += row_stats.total_get_qps;
        total_multi_get_qps += row_stats.total_multi_get_qps;
        total_put_qps += row_stats.total_put_qps;
        total_multi_put_qps += row_stats.total_multi_put_qps;
        total_remove_qps += row_stats.total_remove_qps;
        total_multi_remove_qps += row_stats.total_multi_remove_qps;
        total_incr_qps += row_stats.total_incr_qps;
        total_check_and_set_qps += row_stats.total_check_and_set_qps;
        total_check_and_mutate_qps += row_stats.total_check_and_mutate_qps;
        total_scan_qps += row_stats.total_scan_qps;
        total_dup_qps += row_stats.total_dup_qps;
        total_dup_shipped_ops += row_stats.total_dup_shipped_ops;
        total_dup_failed_shipping_ops = row_stats.total_dup_failed_shipping_ops;
        total_recent_read_cu += row_stats.total_recent_read_cu;
        total_recent_write_cu += row_stats.total_recent_write_cu;
        total_recent_expire_count += row_stats.total_recent_expire_count;
        total_recent_filter_count += row_stats.total_recent_filter_count;
        total_recent_abnormal_count += row_stats.total_recent_abnormal_count;
        total_recent_write_throttling_delay_count +=
            row_stats.total_recent_write_throttling_delay_count;
        total_recent_write_throttling_reject_count +=
            row_stats.total_recent_write_throttling_reject_count;
        total_storage_mb += row_stats.total_storage_mb;
        total_storage_count += row_stats.total_storage_count;
        total_rdb_block_cache_hit_count += row_stats.total_rdb_block_cache_hit_count;
        total_rdb_block_cache_total_count += row_stats.total_rdb_block_cache_total_count;
        total_rdb_index_and_filter_blocks_mem_usage +=
            row_stats.total_rdb_index_and_filter_blocks_mem_usage;
        total_rdb_memtable_mem_usage += row_stats.total_rdb_memtable_mem_usage;
        total_rdb_estimate_num_keys += row_stats.total_rdb_estimate_num_keys;
        total_backup_request_qps += row_stats.total_backup_request_qps;
        total_get_bytes += row_stats.total_get_bytes;
        total_multi_get_bytes += row_stats.total_multi_get_bytes;
        total_scan_bytes += row_stats.total_scan_bytes;
        total_put_bytes += row_stats.total_put_bytes;
        total_multi_put_bytes += row_stats.total_multi_put_bytes;
        total_check_and_set_bytes += row_stats.total_check_and_set_bytes;
        total_check_and_mutate_bytes += row_stats.total_check_and_mutate_bytes;
    }

    std::string app_name;
    double total_get_qps = 0;
    double total_multi_get_qps = 0;
    double total_put_qps = 0;
    double total_multi_put_qps = 0;
    double total_remove_qps = 0;
    double total_multi_remove_qps = 0;
    double total_incr_qps = 0;
    double total_check_and_set_qps = 0;
    double total_check_and_mutate_qps = 0;
    double total_scan_qps = 0;
    double total_dup_qps = 0;
    double total_dup_shipped_ops = 0;
    double total_dup_failed_shipping_ops = 0;
    double total_recent_read_cu = 0;
    double total_recent_write_cu = 0;
    double total_recent_expire_count = 0;
    double total_recent_filter_count = 0;
    double total_recent_abnormal_count = 0;
    double total_recent_write_throttling_delay_count = 0;
    double total_recent_write_throttling_reject_count = 0;
    double total_storage_mb = 0;
    double total_storage_count = 0;
    double total_rdb_block_cache_hit_count = 0;
    double total_rdb_block_cache_total_count = 0;
    double total_rdb_index_and_filter_blocks_mem_usage = 0;
    double total_rdb_memtable_mem_usage = 0;
    double total_rdb_estimate_num_keys = 0;
    double total_backup_request_qps = 0;
    double total_get_bytes = 0;
    double total_multi_get_bytes = 0;
    double total_scan_bytes = 0;
    double total_put_bytes = 0;
    double total_multi_put_bytes = 0;
    double total_check_and_set_bytes = 0;
    double total_check_and_mutate_bytes = 0;
    double max_total_qps = 0;
    double min_total_qps = INT_MAX;
    double max_total_cu = 0;
    double min_total_cu = INT_MAX;
    std::string max_qps_partition_id;
    std::string max_cu_partition_id;
};
