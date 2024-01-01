/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <fmt/core.h>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/write_buffer_manager.h>
#include <stdio.h>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/meta_store.h" // IWYU pragma: keep
#include "common/gpid.h"
#include "hashkey_transform.h"
#include "hotkey_collector.h"
#include "pegasus_event_listener.h"
#include "pegasus_server_impl.h"
#include "pegasus_value_schema.h"
#include "replica_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "runtime/api_layer1.h"
#include "server/capacity_unit_calculator.h" // IWYU pragma: keep
#include "server/key_ttl_compaction_filter.h"
#include "server/pegasus_read_service.h"
#include "server/pegasus_server_write.h" // IWYU pragma: keep
#include "server/range_read_limiter.h"
#include "utils/env.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/strings.h"
#include "utils/token_bucket_throttling_controller.h"

METRIC_DEFINE_counter(replica,
                      get_requests,
                      dsn::metric_unit::kRequests,
                      "The number of GET requests");

METRIC_DEFINE_counter(replica,
                      multi_get_requests,
                      dsn::metric_unit::kRequests,
                      "The number of MULTI_GET requests");

METRIC_DEFINE_counter(replica,
                      batch_get_requests,
                      dsn::metric_unit::kRequests,
                      "The number of BATCH_GET requests");

METRIC_DEFINE_counter(replica,
                      scan_requests,
                      dsn::metric_unit::kRequests,
                      "The number of SCAN requests");

METRIC_DEFINE_percentile_int64(replica,
                               get_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of GET requests");

METRIC_DEFINE_percentile_int64(replica,
                               multi_get_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of MULTI_GET requests");

METRIC_DEFINE_percentile_int64(replica,
                               batch_get_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of BATCH_GET requests");

METRIC_DEFINE_percentile_int64(replica,
                               scan_latency_ns,
                               dsn::metric_unit::kNanoSeconds,
                               "The latency of SCAN requests");

METRIC_DEFINE_counter(replica,
                      read_expired_values,
                      dsn::metric_unit::kValues,
                      "The number of expired values read");

METRIC_DEFINE_counter(replica,
                      read_filtered_values,
                      dsn::metric_unit::kValues,
                      "The number of filtered values read");

METRIC_DEFINE_counter(replica,
                      abnormal_read_requests,
                      dsn::metric_unit::kRequests,
                      "The number of abnormal read requests");

METRIC_DECLARE_counter(throttling_rejected_read_requests);

METRIC_DEFINE_gauge_int64(replica,
                          rdb_total_sst_files,
                          dsn::metric_unit::kFiles,
                          "The total number of rocksdb sst files");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_total_sst_size_mb,
                          dsn::metric_unit::kMegaBytes,
                          "The total size of rocksdb sst files");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_estimated_keys,
                          dsn::metric_unit::kKeys,
                          "The estimated number of rocksdb keys");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_index_and_filter_blocks_mem_usage_bytes,
                          dsn::metric_unit::kBytes,
                          "The memory usage of rocksdb index and filter blocks");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_memtable_mem_usage_bytes,
                          dsn::metric_unit::kBytes,
                          "The memory usage of rocksdb memtables");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_block_cache_hit_count,
                          dsn::metric_unit::kPointLookups,
                          "The hit number of lookups on rocksdb block cache");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_block_cache_total_count,
                          dsn::metric_unit::kPointLookups,
                          "The total number of lookups on rocksdb block cache");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_memtable_hit_count,
                          dsn::metric_unit::kPointLookups,
                          "The hit number of lookups on rocksdb memtable");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_memtable_total_count,
                          dsn::metric_unit::kPointLookups,
                          "The total number of lookups on rocksdb memtable");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_l0_hit_count,
                          dsn::metric_unit::kPointLookups,
                          "The number of lookups served by rocksdb L0");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_l1_hit_count,
                          dsn::metric_unit::kPointLookups,
                          "The number of lookups served by rocksdb L1");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_l2_and_up_hit_count,
                          dsn::metric_unit::kPointLookups,
                          "The number of lookups served by rocksdb L2 and up");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_write_amplification,
                          dsn::metric_unit::kAmplification,
                          "The write amplification of rocksdb");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_read_amplification,
                          dsn::metric_unit::kAmplification,
                          "The read amplification of rocksdb");

// Following metrics are rocksdb statistics that are related to bloom filters.
//
// To measure prefix bloom filters, these metrics are updated after each ::Seek and ::SeekForPrev if
// prefix is enabled and check_filter is set:
// * rdb_bloom_filter_seek_negatives: seek_negatives
// * rdb_bloom_filter_seek_total: seek_negatives + seek_positives
//
// To measure full bloom filters, these metrics are updated after each point lookup. If
// whole_key_filtering is set, this is the result of checking the bloom of the whole key, otherwise
// this is the result of checking the bloom of the prefix:
// * rdb_bloom_filter_point_lookup_negatives: [true] negatives
// * rdb_bloom_filter_point_lookup_positives: positives
// * rdb_bloom_filter_point_lookup_true_positives: true positives
//
// For details please see https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#statistic.

METRIC_DEFINE_gauge_int64(replica,
                          rdb_bloom_filter_seek_negatives,
                          dsn::metric_unit::kSeeks,
                          "The number of times the check for prefix bloom filter was useful in "
                          "avoiding iterator creation (and thus likely IOPs), used by rocksdb for "
                          "each replica");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_bloom_filter_seek_total,
                          dsn::metric_unit::kSeeks,
                          "The number of times prefix bloom filter was checked before creating "
                          "iterator on a file, used by rocksdb");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_bloom_filter_point_lookup_negatives,
                          dsn::metric_unit::kPointLookups,
                          "The number of times full bloom filter has avoided file reads (i.e., "
                          "negatives), used by rocksdb");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_bloom_filter_point_lookup_positives,
                          dsn::metric_unit::kPointLookups,
                          "The number of times full bloom filter has not avoided the reads, used "
                          "by rocksdb");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_bloom_filter_point_lookup_true_positives,
                          dsn::metric_unit::kPointLookups,
                          "The number of times full bloom filter has not avoided the reads and "
                          "data actually exist, used by rocksdb");

METRIC_DEFINE_gauge_int64(server,
                          rdb_block_cache_mem_usage_bytes,
                          dsn::metric_unit::kBytes,
                          "The memory usage of rocksdb block cache");

METRIC_DEFINE_gauge_int64(server,
                          rdb_write_rate_limiter_through_bytes_per_sec,
                          dsn::metric_unit::kBytesPerSec,
                          "The through bytes per second that go through the rate limiter which "
                          "takes control of the write rate of flush and compaction of rocksdb");

DSN_DEFINE_int64(
    pegasus.server,
    rocksdb_limiter_max_write_megabytes_per_sec,
    500,
    "max rate of rocksdb flush and compaction(MB/s), if less than or equal to 0 means close limit");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_background_flushes,
                 4,
                 "Corresponding to RocksDB's options.max_background_flushes, the flush threads are "
                 "shared among all RocksDB's instances in the process");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_background_compactions,
                 12,
                 "Corresponding to RocksDB's options.max_background_compactions, compaction "
                 "threads are shared among all rocksdb instances in the process");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_write_buffer_number,
                 3,
                 "Corresponding to RocksDB's options.max_write_buffer_number");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_num_levels,
                 6,
                 "Corresponding to RocksDB's options.num_levels");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_target_file_size_multiplier,
                 1,
                 "Corresponding to RocksDB's options.target_file_size_multiplier");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_level0_file_num_compaction_trigger,
                 4,
                 "Corresponding to RocksDB's options.level0_file_num_compaction_trigger");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_level0_slowdown_writes_trigger,
                 30,
                 "Corresponding to RocksDB's options.level0_slowdown_writes_trigger");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_level0_stop_writes_trigger,
                 60,
                 "Corresponding to RocksDB's options.level0_stop_writes_trigger");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_block_cache_num_shard_bits,
                 -1,
                 "The number of shard bits of the block cache, it means the block cache is sharded "
                 "into 2^n shards to reduce lock contention. -1 means automatically determined");

// COMPATIBILITY ATTENTION:
// Although old releases would see the new structure as corrupt filter data and read the
// table as if there's no filter, we've decided only to enable the new Bloom filter with new
// format_version=5. This provides a smooth path for automatic adoption over time, with an
// option for early opt-in.
// Reference from rocksdb commit:
// https://github.com/facebook/rocksdb/commit/f059c7d9b96300091e07429a60f4ad55dac84859
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_format_version,
                 2,
                 "block based table data format version, only 2 and 5 is supported in Pegasus. 2 "
                 "is the old version, 5 is the new version supported since rocksdb v6.6.4");
DSN_DEFINE_validator(rocksdb_format_version,
                     [](int32_t value) -> bool { return value == 2 || value == 5; });

DSN_DEFINE_bool(pegasus.server,
                rocksdb_limiter_enable_auto_tune,
                false,
                "whether to enable write rate auto tune when open rocksdb write limit");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_use_direct_reads,
                false,
                "Corresponding to RocksDB's options.use_direct_reads");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_use_direct_io_for_flush_and_compaction,
                false,
                "Corresponding to RocksDB's options.use_direct_io_for_flush_and_compaction");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_disable_table_block_cache,
                false,
                "Whether to disable RocksDB's block cache");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_enable_write_buffer_manager,
                false,
                "enable write buffer manager to limit total memory used by memtables and block "
                "caches across multiple replicas");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_partition_filters,
                false,
                "Note: currently this option requires two_level_index_search to be set as well. "
                "Use partitioned full filters for each SST file. This option is incompatibile with "
                "block-based filters.");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_cache_index_and_filter_blocks,
                false,
                "Indicating if we'd put index/filter blocks to the block cache. If not specified, "
                "each \"table reader\" object will pre-load index/filter block during table "
                "initialization.");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_pin_top_level_index_and_filter,
                true,
                "If cache_index_and_filter_blocks is true and the below is true, then the "
                "top-level index of partitioned filter and index blocks are stored in the cache, "
                "but a reference is held in the \"table reader\" object so the blocks are pinned "
                "and only evicted from cache when the table reader is freed. This is not limited "
                "to l0 in LSM tree.");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_cache_index_and_filter_blocks_with_high_priority,
                true,
                "If cache_index_and_filter_blocks is enabled, cache index and filter blocks with "
                "high priority. If set to true, depending on implementation of block cache, index "
                "and filter blocks may be less likely to be evicted than data blocks.");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_pin_l0_filter_and_index_blocks_in_cache,
                false,
                "if cache_index_and_filter_blocks is true and the below is true, then filter and "
                "index blocks are stored in the cache, but a reference is held in the \"table "
                "reader\" object so the blocks are pinned and only evicted from cache when the "
                "table reader is freed.");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_disable_bloom_filter,
                false,
                "Whether to disable RocksDB bloom filter");
// If used, For every data block we load into memory, we will create a bitmap
// of size ((block_size / `read_amp_bytes_per_bit`) / 8) bytes. This bitmap
// will be used to figure out the percentage we actually read of the blocks.
//
// When this feature is used Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES and
// Tickers::READ_AMP_TOTAL_READ_BYTES can be used to calculate the
// read amplification using this formula
// (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
//
// value  =>  memory usage (percentage of loaded blocks memory)
// 1      =>  12.50 %
// 2      =>  06.25 %
// 4      =>  03.12 %
// 8      =>  01.56 %
// 16     =>  00.78 %
//
// Note: This number must be a power of 2, if not it will be sanitized
// to be the next lowest power of 2, for example a value of 7 will be
// treated as 4, a value of 19 will be treated as 16.
//
// Default: 0 (disabled)
// see https://github.com/facebook/rocksdb/blob/v6.6.4/include/rocksdb/table.h#L247
DSN_DEFINE_int32(pegasus.server,
                 read_amp_bytes_per_bit,
                 0,
                 "config for using to calculate the read amplification, must be a power of 2, zero "
                 "means disable count read amplification");

DSN_DEFINE_validator(read_amp_bytes_per_bit, [](const int64_t read_amp_bytes_per_bit) -> bool {
    return read_amp_bytes_per_bit == 0 ||
           (read_amp_bytes_per_bit > 0 &&
            (read_amp_bytes_per_bit & (read_amp_bytes_per_bit - 1)) == 0);
});

DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_abnormal_batch_get_bytes_threshold,
                  1e7,
                  "batch-get operation total key-value bytes size exceed this "
                  "threshold will be logged, 0 means no check");
DSN_TAG_VARIABLE(rocksdb_abnormal_batch_get_bytes_threshold, FT_MUTABLE);

DSN_DEFINE_uint64(
    pegasus.server,
    rocksdb_abnormal_batch_get_count_threshold,
    2000,
    "batch-get operation iterate count exceed this threshold will be logged, 0 means no check");
DSN_TAG_VARIABLE(rocksdb_abnormal_batch_get_count_threshold, FT_MUTABLE);

// In production environment, it has been observed that an instance of rocksdb about 20GB in total
// size which has run beyond 199 days, generated a big log file sized 96MB, with 492KB for each
// day.
//
// Accordingly, default value for `rocksdb_log_file_time_to_roll` can be set to one day, and
// that for `rocksdb_keep_log_file_num` can be set to 32, which means log files for recent one
// month will be reserved.
//
// On the other hand, the max size of a log file is restricted to 8MB. In practice, the size of
// logs over a day tends to be less than 1MB; however, once errors are reported very frequently,
// the log file will grow larger and go far beyond several hundreds of KB.
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_max_log_file_size,
                  8 * 1024 * 1024,
                  "specify the maximal size of the info log file: once the log file is larger "
                  "than this option, a new info log file will be created; if this option is set "
                  "to 0, all logs will be written to one log file.");

DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_log_file_time_to_roll,
                  24 * 60 * 60,
                  "specify time for the info log file to roll (in seconds): if this option is "
                  "specified with non-zero value, log file will be rolled if it has been active "
                  "longer than this option; otherwise, if this options is set to 0, log file will "
                  "never be rolled by life time");

DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_keep_log_file_num,
                  32,
                  "specify the maximal numbers of info log files to be kept: once the number of "
                  "info logs goes beyond this option, stale log files will be cleaned.");

DSN_DEFINE_uint32(pegasus.server,
                  rocksdb_multi_get_max_iteration_count,
                  3000,
                  "max iteration count for each range read for multi-get operation, if exceed this "
                  "threshold, iterator will be stopped.");
DSN_DEFINE_uint32(pegasus.server,
                  rocksdb_max_iteration_count,
                  1000,
                  "max iteration count for each range read, if exceed this threshold, iterator "
                  "will be stopped.");
DSN_DEFINE_uint32(pegasus.server,
                  checkpoint_reserve_min_count,
                  2,
                  "Minimum count of checkpoint to reserve.");
DSN_TAG_VARIABLE(checkpoint_reserve_min_count, FT_MUTABLE);
DSN_DEFINE_uint32(pegasus.server,
                  checkpoint_reserve_time_seconds,
                  1800,
                  "Minimum seconds of checkpoint to reserve, 0 means no check.");
DSN_TAG_VARIABLE(checkpoint_reserve_time_seconds, FT_MUTABLE);
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_open_files,
                 -1,
                 "The number of opened files that can be used by a replica(namely a DB instance). "
                 "The default value is -1 which means always keep files opened.");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_slow_query_threshold_ns,
                  100000000,
                  "get/multi-get operation duration exceed this threshold will be logged");
DSN_DEFINE_validator(rocksdb_slow_query_threshold_ns,
                     [](uint64_t value) -> bool { return value > 0; });
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_abnormal_get_size_threshold,
                  1000000,
                  "A warning log will be print if the key-value size of Get operation is larger "
                  "than this config, 0 means never print");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_abnormal_multi_get_size_threshold,
                  10000000,
                  "A warning log will be print if the total key-value size of Multi-Get operation "
                  "is larger than this config, 0 means never print");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_abnormal_multi_get_iterate_count_threshold,
                  1000,
                  "A warning log will be print if the scan iteration count of Multi-Get operation "
                  "is larger than this config, 0 means never print");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_multi_get_max_iteration_size,
                  30 << 20,
                  "multi-get operation total key-value size exceed this threshold will stop "
                  "iterating rocksdb, 0 means no check");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_iteration_threshold_time_ms,
                  30000,
                  "max duration for handling one pegasus scan request(sortkey_count/multiget/scan) "
                  "if exceed this threshold, iterator will be stopped, 0 means no check");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_compaction_readahead_size,
                  2 * 1024 * 1024,
                  "Corresponding to RocksDB's options.compaction_readahead_size");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_writable_file_max_buffer_size,
                  1024 * 1024,
                  "Corresponding to RocksDB's options.writable_file_max_buffer_size");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_write_buffer_size,
                  64 * 1024 * 1024,
                  "Corresponding to RocksDB's options.write_buffer_size");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_target_file_size_base,
                  64 * 1024 * 1024,
                  "Corresponding to RocksDB's options.target_file_size_base");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_max_bytes_for_level_base,
                  10 * 64 * 1024 * 1024,
                  "Corresponding to RocksDB's options.max_bytes_for_level_base");
DSN_DEFINE_uint64(
    pegasus.server,
    rocksdb_block_cache_capacity,
    10 * 1024 * 1024 * 1024ULL,
    "The Block Cache capacity shared by all RocksDB instances in the process, in bytes");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_total_size_across_write_buffer,
                  0,
                  "total size limit used by memtables across multiple replicas");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_metadata_block_size,
                  4096,
                  "Block size for partitioned metadata. Currently applied to indexes when "
                  "two_level_index_search is used and to filters when partition_filters is used. "
                  "Note: Since in the current implementation the filters and index partitions "
                  "are aligned, an index/filter block is created when either index or filter "
                  "block size reaches the specified limit. "
                  "Note: this limit is currently applied to only index blocks; a filter "
                  "partition is cut right after an index block is cut");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_periodic_compaction_seconds,
                  0,
                  "periodic_compaction_seconds, 0 means no periodic compaction");
DSN_DEFINE_double(pegasus.server,
                  rocksdb_max_bytes_for_level_multiplier,
                  10,
                  "Corresponding to RocksDB's options.rocksdb_max_bytes_for_level_multiplier");
DSN_DEFINE_double(pegasus.server,
                  rocksdb_bloom_filter_bits_per_key,
                  10,
                  "average bits allocated per key in bloom filter");
DSN_DEFINE_string(pegasus.server,
                  rocksdb_compression_type,
                  "lz4",
                  "Corresponding to RocksDB's options.compression. Available config: "
                  "'[none|snappy|zstd|lz4]' for all level 1 and higher levels, and "
                  "'per_level:[none|snappy|zstd|lz4],[none|snappy|zstd|lz4],...' for each level "
                  "0,1,..., the last compression type will be used for levels not specified in the "
                  "list.");
DSN_DEFINE_string(pegasus.server,
                  rocksdb_index_type,
                  "binary_search",
                  "The index type that will be used for this table.");
DSN_DEFINE_string(pegasus.server,
                  rocksdb_filter_type,
                  "prefix",
                  "Bloom filter type, should be either 'common' or 'prefix'");
DSN_DEFINE_validator(rocksdb_filter_type, [](const char *value) -> bool {
    return dsn::utils::equals(value, "common") || dsn::utils::equals(value, "prefix");
});
DSN_DEFINE_uint64(pegasus.server,
                  stats_dump_period_sec,
                  600, // 600 is the default value in RocksDB.
                  "If not zero, dump rocksdb.stats to RocksDB LOG every stats_dump_period_sec");
DSN_DEFINE_uint64(pegasus.server,
                  stats_persist_period_sec,
                  600, // 600 is the default value in RocksDB.
                  "If not zero, dump rocksdb.stats to RocksDB every stats_persist_period_sec");

namespace dsn {
namespace replication {
class replica;
} // namespace replication
} // namespace dsn
namespace pegasus {
namespace server {
static const std::unordered_map<std::string, rocksdb::BlockBasedTableOptions::IndexType>
    INDEX_TYPE_STRING_MAP = {
        {"binary_search", rocksdb::BlockBasedTableOptions::IndexType::kBinarySearch},
        {"hash_search", rocksdb::BlockBasedTableOptions::IndexType::kHashSearch},
        {"two_level_index_search",
         rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch},
        {"binary_search_with_first_key",
         rocksdb::BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey}};

pegasus_server_impl::pegasus_server_impl(dsn::replication::replica *r)
    : pegasus_read_service(r),
      _db(nullptr),
      _data_cf(nullptr),
      _meta_cf(nullptr),
      _is_open(false),
      _pegasus_data_version(PEGASUS_DATA_VERSION_MAX),
      _last_durable_decree(0),
      _is_checkpointing(false),
      _manual_compact_svc(this),
      _partition_version(0),
      METRIC_VAR_INIT_replica(get_requests),
      METRIC_VAR_INIT_replica(multi_get_requests),
      METRIC_VAR_INIT_replica(batch_get_requests),
      METRIC_VAR_INIT_replica(scan_requests),
      METRIC_VAR_INIT_replica(get_latency_ns),
      METRIC_VAR_INIT_replica(multi_get_latency_ns),
      METRIC_VAR_INIT_replica(batch_get_latency_ns),
      METRIC_VAR_INIT_replica(scan_latency_ns),
      METRIC_VAR_INIT_replica(read_expired_values),
      METRIC_VAR_INIT_replica(read_filtered_values),
      METRIC_VAR_INIT_replica(abnormal_read_requests),
      METRIC_VAR_INIT_replica(throttling_rejected_read_requests),
      METRIC_VAR_INIT_replica(rdb_total_sst_files),
      METRIC_VAR_INIT_replica(rdb_total_sst_size_mb),
      METRIC_VAR_INIT_replica(rdb_estimated_keys),
      METRIC_VAR_INIT_replica(rdb_index_and_filter_blocks_mem_usage_bytes),
      METRIC_VAR_INIT_replica(rdb_memtable_mem_usage_bytes),
      METRIC_VAR_INIT_replica(rdb_block_cache_hit_count),
      METRIC_VAR_INIT_replica(rdb_block_cache_total_count),
      METRIC_VAR_INIT_replica(rdb_memtable_hit_count),
      METRIC_VAR_INIT_replica(rdb_memtable_total_count),
      METRIC_VAR_INIT_replica(rdb_l0_hit_count),
      METRIC_VAR_INIT_replica(rdb_l1_hit_count),
      METRIC_VAR_INIT_replica(rdb_l2_and_up_hit_count),
      METRIC_VAR_INIT_replica(rdb_write_amplification),
      METRIC_VAR_INIT_replica(rdb_read_amplification),
      METRIC_VAR_INIT_replica(rdb_bloom_filter_seek_negatives),
      METRIC_VAR_INIT_replica(rdb_bloom_filter_seek_total),
      METRIC_VAR_INIT_replica(rdb_bloom_filter_point_lookup_negatives),
      METRIC_VAR_INIT_replica(rdb_bloom_filter_point_lookup_positives),
      METRIC_VAR_INIT_replica(rdb_bloom_filter_point_lookup_true_positives)
{
    _primary_host_port = dsn_primary_host_port().to_string();
    _gpid = get_gpid();

    _read_hotkey_collector =
        std::make_shared<hotkey_collector>(dsn::replication::hotkey_type::READ, this);
    _write_hotkey_collector =
        std::make_shared<hotkey_collector>(dsn::replication::hotkey_type::WRITE, this);

    _read_size_throttling_controller =
        std::make_shared<dsn::utils::token_bucket_throttling_controller>();
    _slow_query_threshold_ns = FLAGS_rocksdb_slow_query_threshold_ns;
    _rng_rd_opts.multi_get_max_iteration_count = FLAGS_rocksdb_multi_get_max_iteration_count;
    _rng_rd_opts.multi_get_max_iteration_size = FLAGS_rocksdb_multi_get_max_iteration_size;
    _rng_rd_opts.rocksdb_max_iteration_count = FLAGS_rocksdb_max_iteration_count;
    _rng_rd_opts.rocksdb_iteration_threshold_time_ms = FLAGS_rocksdb_iteration_threshold_time_ms;

    // init rocksdb::DBOptions
    _db_opts.env = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive);
    _db_opts.create_if_missing = true;
    // atomic flush data CF and meta CF, aim to keep consistency of 'last flushed decree' in meta CF
    // and data in data CF.
    _db_opts.atomic_flush = true;
    _db_opts.use_direct_reads = FLAGS_rocksdb_use_direct_reads;
    _db_opts.use_direct_io_for_flush_and_compaction =
        FLAGS_rocksdb_use_direct_io_for_flush_and_compaction;
    _db_opts.compaction_readahead_size = FLAGS_rocksdb_compaction_readahead_size;
    _db_opts.writable_file_max_buffer_size = FLAGS_rocksdb_writable_file_max_buffer_size;

    _statistics = rocksdb::CreateDBStatistics();
    _statistics->set_stats_level(rocksdb::kExceptDetailedTimers);
    _db_opts.statistics = _statistics;

    _db_opts.listeners.emplace_back(new pegasus_event_listener(this));
    _db_opts.max_background_flushes = FLAGS_rocksdb_max_background_flushes;
    _db_opts.max_background_compactions = FLAGS_rocksdb_max_background_compactions;
    _db_opts.stats_dump_period_sec = FLAGS_stats_dump_period_sec;
    _db_opts.stats_persist_period_sec = FLAGS_stats_persist_period_sec;
    // init rocksdb::ColumnFamilyOptions for data column family
    _data_cf_opts.write_buffer_size = FLAGS_rocksdb_write_buffer_size;
    _data_cf_opts.max_write_buffer_number = FLAGS_rocksdb_max_write_buffer_number;
    _data_cf_opts.num_levels = FLAGS_rocksdb_num_levels;
    _data_cf_opts.target_file_size_base = FLAGS_rocksdb_target_file_size_base;
    _data_cf_opts.target_file_size_multiplier = FLAGS_rocksdb_target_file_size_multiplier;
    _data_cf_opts.max_bytes_for_level_base = FLAGS_rocksdb_max_bytes_for_level_base;
    _data_cf_opts.max_bytes_for_level_multiplier = FLAGS_rocksdb_max_bytes_for_level_multiplier;

    // we need set max_compaction_bytes definitely because set_usage_scenario() depends on it.
    _data_cf_opts.max_compaction_bytes = _data_cf_opts.target_file_size_base * 25;
    _data_cf_opts.level0_file_num_compaction_trigger =
        FLAGS_rocksdb_level0_file_num_compaction_trigger;
    _data_cf_opts.level0_slowdown_writes_trigger = FLAGS_rocksdb_level0_slowdown_writes_trigger;
    _data_cf_opts.level0_stop_writes_trigger = FLAGS_rocksdb_level0_stop_writes_trigger;

    CHECK(parse_compression_types(FLAGS_rocksdb_compression_type,
                                  _data_cf_opts.compression_per_level),
          "parse rocksdb_compression_type failed.");

    _meta_cf_opts = _data_cf_opts;
    // Set level0_file_num_compaction_trigger of meta CF as 10 to reduce frequent compaction.
    _meta_cf_opts.level0_file_num_compaction_trigger = 10;
    // Data in meta CF is very little, disable compression to save CPU load.
    CHECK(parse_compression_types("none", _meta_cf_opts.compression_per_level),
          "parse rocksdb_compression_type failed.");

    _tbl_opts.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;

    if (FLAGS_rocksdb_disable_table_block_cache) {
        _tbl_opts.no_block_cache = true;
        _tbl_opts.block_restart_interval = 4;
    } else {
        // If block cache is enabled, all replicas on this server will share the same block cache
        // object. It's convenient to control the total memory used by this server, and the LRU
        // algorithm used by the block cache object can be more efficient in this way.
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            // init block cache
            _s_block_cache = rocksdb::NewLRUCache(FLAGS_rocksdb_block_cache_capacity,
                                                  FLAGS_rocksdb_block_cache_num_shard_bits);
        });

        // every replica has the same block cache
        _tbl_opts.block_cache = _s_block_cache;
    }

    // FLAGS_rocksdb_limiter_max_write_megabytes_per_sec <= 0 means close the rate limit.
    // For more detail arguments see
    // https://github.com/facebook/rocksdb/blob/v6.6.4/include/rocksdb/rate_limiter.h#L111-L137
    if (FLAGS_rocksdb_limiter_max_write_megabytes_per_sec > 0) {
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            _s_rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(rocksdb::NewGenericRateLimiter(
                FLAGS_rocksdb_limiter_max_write_megabytes_per_sec << 20,
                100 * 1000, // refill_period_us
                10,         // fairness
                rocksdb::RateLimiter::Mode::kWritesOnly,
                FLAGS_rocksdb_limiter_enable_auto_tune));
        });
        _db_opts.rate_limiter = _s_rate_limiter;
    }

    LOG_INFO_PREFIX("rocksdb_enable_write_buffer_manager = {}",
                    FLAGS_rocksdb_enable_write_buffer_manager);
    if (FLAGS_rocksdb_enable_write_buffer_manager) {
        // If write buffer manager is enabled, all replicas(one DB instance for each
        // replica) on this server will share the same write buffer manager object,
        // thus the same block cache object. It's convenient to control the total memory
        // of memtables and block caches used by this server.
        //
        // While write buffer manager is enabled, FLAGS_rocksdb_total_size_across_write_buffer = 0
        // indicates no limit on memory, for details see:
        // https://github.com/facebook/rocksdb/blob/v6.6.4/include/rocksdb/write_buffer_manager.h#L23-24
        static std::once_flag flag;
        std::call_once(flag, [&]() {
            LOG_INFO_PREFIX("rocksdb_total_size_across_write_buffer = {}",
                            FLAGS_rocksdb_total_size_across_write_buffer);
            _s_write_buffer_manager = std::make_shared<rocksdb::WriteBufferManager>(
                static_cast<size_t>(FLAGS_rocksdb_total_size_across_write_buffer),
                _tbl_opts.block_cache);
        });
        _db_opts.write_buffer_manager = _s_write_buffer_manager;
    }

    _db_opts.max_open_files = FLAGS_rocksdb_max_open_files;
    LOG_INFO_PREFIX("rocksdb_max_open_files = {}", _db_opts.max_open_files);

    _db_opts.max_log_file_size = static_cast<size_t>(FLAGS_rocksdb_max_log_file_size);
    LOG_INFO_PREFIX("rocksdb_max_log_file_size = {}", _db_opts.max_log_file_size);

    _db_opts.log_file_time_to_roll = static_cast<size_t>(FLAGS_rocksdb_log_file_time_to_roll);
    LOG_INFO_PREFIX("rocksdb_log_file_time_to_roll = {}", _db_opts.log_file_time_to_roll);

    _db_opts.keep_log_file_num = static_cast<size_t>(FLAGS_rocksdb_keep_log_file_num);
    LOG_INFO_PREFIX("rocksdb_keep_log_file_num = {}", _db_opts.keep_log_file_num);

    auto index_type_item = INDEX_TYPE_STRING_MAP.find(FLAGS_rocksdb_index_type);
    CHECK(index_type_item != INDEX_TYPE_STRING_MAP.end(),
          "[pegasus.server]rocksdb_index_type should be one among binary_search, "
          "hash_search, two_level_index_search or binary_search_with_first_key.");
    _tbl_opts.index_type = index_type_item->second;
    LOG_INFO_PREFIX("rocksdb_index_type = {}", FLAGS_rocksdb_index_type);

    _tbl_opts.partition_filters = FLAGS_rocksdb_partition_filters;
    // TODO(yingchun): clean up these useless log ?
    LOG_INFO_PREFIX("rocksdb_partition_filters = {}", _tbl_opts.partition_filters);

    _tbl_opts.metadata_block_size = FLAGS_rocksdb_metadata_block_size;
    LOG_INFO_PREFIX("rocksdb_metadata_block_size = {}", _tbl_opts.metadata_block_size);

    _tbl_opts.cache_index_and_filter_blocks = FLAGS_rocksdb_cache_index_and_filter_blocks;
    LOG_INFO_PREFIX("rocksdb_cache_index_and_filter_blocks = {}",
                    _tbl_opts.cache_index_and_filter_blocks);

    _tbl_opts.pin_top_level_index_and_filter = FLAGS_rocksdb_pin_top_level_index_and_filter;
    LOG_INFO_PREFIX("rocksdb_pin_top_level_index_and_filter = {}",
                    _tbl_opts.pin_top_level_index_and_filter);

    _tbl_opts.cache_index_and_filter_blocks_with_high_priority =
        FLAGS_rocksdb_cache_index_and_filter_blocks_with_high_priority;
    LOG_INFO_PREFIX("rocksdb_cache_index_and_filter_blocks_with_high_priority = {}",
                    _tbl_opts.cache_index_and_filter_blocks_with_high_priority);

    _tbl_opts.pin_l0_filter_and_index_blocks_in_cache =
        FLAGS_rocksdb_pin_l0_filter_and_index_blocks_in_cache;
    LOG_INFO_PREFIX("rocksdb_pin_l0_filter_and_index_blocks_in_cache = {}",
                    _tbl_opts.pin_l0_filter_and_index_blocks_in_cache);

    // Bloom filter configurations.
    if (!FLAGS_rocksdb_disable_bloom_filter) {
        // average bits allocated per key in bloom filter.
        // FLAGS_rocksdb_bloom_filter_bits_per_key    |           false positive rate
        // -------------------------------------------+-------------------------------------------
        //                                            | format_version < 5 | format_version = 5
        //                                  6         |      5.70953       |      5.69888
        //                                  8         |      2.45766       |      2.29709
        //                                 10         |      1.13977       |      0.959254
        //                                 12         |      0.662498      |      0.411593
        //                                 16         |      0.353023      |      0.0873754
        //                                 24         |      0.261552      |      0.0060971
        //                                 50         |      0.225453      |      ~0.00003
        // Recommend using no more than three decimal digits after the decimal point, as in 6.667.
        // More details: https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter
        _tbl_opts.format_version = FLAGS_rocksdb_format_version;
        _tbl_opts.filter_policy.reset(
            rocksdb::NewBloomFilterPolicy(FLAGS_rocksdb_bloom_filter_bits_per_key, false));

        if (dsn::utils::equals(FLAGS_rocksdb_filter_type, "prefix")) {
            _data_cf_opts.prefix_extractor.reset(new HashkeyTransform());
            _data_cf_opts.memtable_prefix_bloom_size_ratio = 0.1;

            _data_cf_rd_opts.prefix_same_as_start = true;
        }
    }

    _data_cf_opts.table_factory.reset(NewBlockBasedTableFactory(_tbl_opts));
    _meta_cf_opts.table_factory.reset(NewBlockBasedTableFactory(_tbl_opts));

    _key_ttl_compaction_filter_factory = std::make_shared<KeyWithTTLCompactionFilterFactory>();
    _data_cf_opts.compaction_filter_factory = _key_ttl_compaction_filter_factory;
    _data_cf_opts.periodic_compaction_seconds = FLAGS_rocksdb_periodic_compaction_seconds;
    _checkpoint_reserve_min_count = FLAGS_checkpoint_reserve_min_count;
    _checkpoint_reserve_time_seconds = FLAGS_checkpoint_reserve_time_seconds;

    // TODO: move the qps/latency counters and it's statistics to replication_app_base layer
    std::string str_gpid = _gpid.to_string();
    char name[256];

    // These counters are singletons on this server shared by all replicas, so we initialize
    // them only once.
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        METRIC_VAR_ASSIGN_server(rdb_block_cache_mem_usage_bytes);
        METRIC_VAR_ASSIGN_server(rdb_write_rate_limiter_through_bytes_per_sec);
    });
}

} // namespace server
} // namespace pegasus
