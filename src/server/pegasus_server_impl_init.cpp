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
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "hashkey_transform.h"
#include "hotkey_collector.h"
#include "pegasus_event_listener.h"
#include "pegasus_server_impl.h"
#include "pegasus_value_schema.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "replica_admin_types.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "server/capacity_unit_calculator.h" // IWYU pragma: keep
#include "server/key_ttl_compaction_filter.h"
#include "server/meta_store.h" // IWYU pragma: keep
#include "server/pegasus_read_service.h"
#include "server/pegasus_server_write.h" // IWYU pragma: keep
#include "server/range_read_limiter.h"
#include "utils/env.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/token_bucket_throttling_controller.h"

namespace dsn {
namespace replication {
class replica;
} // namespace replication
} // namespace dsn

namespace pegasus {
namespace server {

DSN_DEFINE_int64(
    pegasus.server,
    rocksdb_limiter_max_write_megabytes_per_sec,
    500,
    "max rate of rocksdb flush and compaction(MB/s), if less than or equal to 0 means close limit");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_background_flushes,
                 4,
                 "rocksdb options.max_background_flushes, flush threads are shared among all "
                 "rocksdb instances in one process");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_background_compactions,
                 12,
                 "rocksdb options.max_background_compactions, compaction threads are shared among "
                 "all rocksdb instances in one process");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_max_write_buffer_number,
                 3,
                 "rocksdb options.max_write_buffer_number");
DSN_DEFINE_int32(pegasus.server, rocksdb_num_levels, 6, "rocksdb options.num_levels");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_target_file_size_multiplier,
                 1,
                 "rocksdb options.target_file_size_multiplier");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_level0_file_num_compaction_trigger,
                 4,
                 "rocksdb options.level0_file_num_compaction_trigger");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_level0_slowdown_writes_trigger,
                 30,
                 "rocksdb options.level0_slowdown_writes_trigger, default 30");
DSN_DEFINE_int32(pegasus.server,
                 rocksdb_level0_stop_writes_trigger,
                 60,
                 "rocksdb options.level0_stop_writes_trigger");
DSN_DEFINE_int32(
    pegasus.server,
    rocksdb_block_cache_num_shard_bits,
    -1,
    "block cache will be sharded into 2^num_shard_bits shards, default value is -1(auto)");

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
                "rocksdb options.use_direct_reads");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_use_direct_io_for_flush_and_compaction,
                false,
                "rocksdb options.use_direct_io_for_flush_and_compaction");
DSN_DEFINE_bool(pegasus.server,
                rocksdb_disable_table_block_cache,
                false,
                "rocksdb _tbl_opts.no_block_cache");
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
                "Whether to disable bloom filter");
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
DSN_DEFINE_uint32(pegasus.server,
                  checkpoint_reserve_time_seconds,
                  1800,
                  "Minimum seconds of checkpoint to reserve, 0 means no check.");
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
DSN_DEFINE_uint64(
    pegasus.server,
    rocksdb_abnormal_get_size_threshold,
    1000000,
    "get operation value size exceed this threshold will be logged, 0 means no check");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_abnormal_multi_get_size_threshold,
                  10000000,
                  "multi-get operation total key-value size exceed this threshold will be logged, "
                  "0 means no check");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_abnormal_multi_get_iterate_count_threshold,
                  1000,
                  "multi-get operation iterate count exceed this threshold will be logged, 0 means "
                  "no check");
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
                  "rocksdb options.compaction_readahead_size");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_writable_file_max_buffer_size,
                  1024 * 1024,
                  "rocksdb options.writable_file_max_buffer_size");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_write_buffer_size,
                  64 * 1024 * 1024,
                  "rocksdb options.write_buffer_size");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_target_file_size_base,
                  64 * 1024 * 1024,
                  "rocksdb options.target_file_size_base");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_max_bytes_for_level_base,
                  10 * 64 * 1024 * 1024,
                  "rocksdb options.max_bytes_for_level_base");
DSN_DEFINE_uint64(pegasus.server,
                  rocksdb_block_cache_capacity,
                  10 * 1024 * 1024 * 1024ULL,
                  "block cache capacity for one pegasus server, shared by all rocksdb instances");
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
                  "rocksdb options.rocksdb_max_bytes_for_level_multiplier");
DSN_DEFINE_double(pegasus.server,
                  rocksdb_bloom_filter_bits_per_key,
                  10,
                  "average bits allocated per key in bloom filter");
DSN_DEFINE_string(pegasus.server,
                  rocksdb_compression_type,
                  "lz4",
                  "rocksdb options.compression. Available config: '[none|snappy|zstd|lz4]' for all "
                  "level 2 and higher levels, and "
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
      _partition_version(0)
{
    _primary_address = dsn::rpc_address(dsn_primary_address()).to_string();
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

    // register the perf counters
    snprintf(name, 255, "get_qps@%s", str_gpid.c_str());
    _pfc_get_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of GET request");

    snprintf(name, 255, "multi_get_qps@%s", str_gpid.c_str());
    _pfc_multi_get_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of MULTI_GET request");

    snprintf(name, 255, "batch_get_qps@%s", str_gpid.c_str());
    _pfc_batch_get_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of BATCH_GET request");

    snprintf(name, 255, "scan_qps@%s", str_gpid.c_str());
    _pfc_scan_qps.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the qps of SCAN request");

    snprintf(name, 255, "get_latency@%s", str_gpid.c_str());
    _pfc_get_latency.init_app_counter("app.pegasus",
                                      name,
                                      COUNTER_TYPE_NUMBER_PERCENTILES,
                                      "statistic the latency of GET request");

    snprintf(name, 255, "multi_get_latency@%s", str_gpid.c_str());
    _pfc_multi_get_latency.init_app_counter("app.pegasus",
                                            name,
                                            COUNTER_TYPE_NUMBER_PERCENTILES,
                                            "statistic the latency of MULTI_GET request");

    snprintf(name, 255, "batch_get_latency@%s", str_gpid.c_str());
    _pfc_batch_get_latency.init_app_counter("app.pegasus",
                                            name,
                                            COUNTER_TYPE_NUMBER_PERCENTILES,
                                            "statistic the latency of BATCH_GET request");

    snprintf(name, 255, "scan_latency@%s", str_gpid.c_str());
    _pfc_scan_latency.init_app_counter("app.pegasus",
                                       name,
                                       COUNTER_TYPE_NUMBER_PERCENTILES,
                                       "statistic the latency of SCAN request");

    snprintf(name, 255, "recent.expire.count@%s", str_gpid.c_str());
    _pfc_recent_expire_count.init_app_counter("app.pegasus",
                                              name,
                                              COUNTER_TYPE_VOLATILE_NUMBER,
                                              "statistic the recent expired value read count");

    snprintf(name, 255, "recent.filter.count@%s", str_gpid.c_str());
    _pfc_recent_filter_count.init_app_counter("app.pegasus",
                                              name,
                                              COUNTER_TYPE_VOLATILE_NUMBER,
                                              "statistic the recent filtered value read count");

    snprintf(name, 255, "recent.abnormal.count@%s", str_gpid.c_str());
    _pfc_recent_abnormal_count.init_app_counter("app.pegasus",
                                                name,
                                                COUNTER_TYPE_VOLATILE_NUMBER,
                                                "statistic the recent abnormal read count");

    snprintf(name, 255, "disk.storage.sst.count@%s", str_gpid.c_str());
    _pfc_rdb_sst_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the count of sstable files");

    snprintf(name, 255, "disk.storage.sst(MB)@%s", str_gpid.c_str());
    _pfc_rdb_sst_size.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the size of sstable files");

    snprintf(name, 255, "rdb.block_cache.hit_count@%s", str_gpid.c_str());
    _pfc_rdb_block_cache_hit_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the hit count of rocksdb block cache");

    snprintf(name, 255, "rdb.block_cache.total_count@%s", str_gpid.c_str());
    _pfc_rdb_block_cache_total_count.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistic the total count of rocksdb block cache");

    snprintf(name, 255, "rdb.write_amplification@%s", str_gpid.c_str());
    _pfc_rdb_write_amplification.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the write amplification of rocksdb");

    snprintf(name, 255, "rdb.read_amplification@%s", str_gpid.c_str());
    _pfc_rdb_read_amplification.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the read amplification of rocksdb");

    snprintf(name, 255, "rdb.read_memtable_hit_count@%s", str_gpid.c_str());
    _pfc_rdb_memtable_hit_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the read memtable hit count");

    snprintf(name, 255, "rdb.read_memtable_total_count@%s", str_gpid.c_str());
    _pfc_rdb_memtable_total_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the read memtable total count");

    snprintf(name, 255, "rdb.read_l0_hit_count@%s", str_gpid.c_str());
    _pfc_rdb_l0_hit_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the read l0 hit count");

    snprintf(name, 255, "rdb.read_l1_hit_count@%s", str_gpid.c_str());
    _pfc_rdb_l1_hit_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the read l1 hit count");

    snprintf(name, 255, "rdb.read_l2andup_hit_count@%s", str_gpid.c_str());
    _pfc_rdb_l2andup_hit_count.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistics the read l2andup hit count");

    // These counters are singletons on this server shared by all replicas, so we initialize
    // them only once.
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _pfc_rdb_block_cache_mem_usage.init_global_counter(
            "replica",
            "app.pegasus",
            "rdb.block_cache.memory_usage",
            COUNTER_TYPE_NUMBER,
            "statistic the memory usage of rocksdb block cache");

        _pfc_rdb_write_limiter_rate_bytes.init_global_counter(
            "replica",
            "app.pegasus",
            "rdb.write_limiter_rate_bytes",
            COUNTER_TYPE_NUMBER,
            "statistic the through bytes of rocksdb write rate limiter");
    });

    snprintf(name, 255, "rdb.index_and_filter_blocks.memory_usage@%s", str_gpid.c_str());
    _pfc_rdb_index_and_filter_blocks_mem_usage.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistic the memory usage of rocksdb index and filter blocks");

    snprintf(name, 255, "rdb.memtable.memory_usage@%s", str_gpid.c_str());
    _pfc_rdb_memtable_mem_usage.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_NUMBER, "statistic the memory usage of rocksdb memtable");

    snprintf(name, 255, "rdb.estimate_num_keys@%s", str_gpid.c_str());
    _pfc_rdb_estimate_num_keys.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistics the estimated number of keys inside the rocksdb");

    snprintf(name, 255, "rdb.bf_seek_negatives@%s", str_gpid.c_str());
    _pfc_rdb_bf_seek_negatives.init_app_counter("app.pegasus",
                                                name,
                                                COUNTER_TYPE_NUMBER,
                                                "statistics the number of times bloom filter was "
                                                "checked before creating iterator on a file and "
                                                "useful in avoiding iterator creation (and thus "
                                                "likely IOPs)");

    snprintf(name, 255, "rdb.bf_seek_total@%s", str_gpid.c_str());
    _pfc_rdb_bf_seek_total.init_app_counter("app.pegasus",
                                            name,
                                            COUNTER_TYPE_NUMBER,
                                            "statistics the number of times bloom filter was "
                                            "checked before creating iterator on a file");

    snprintf(name, 255, "rdb.bf_point_positive_true@%s", str_gpid.c_str());
    _pfc_rdb_bf_point_positive_true.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistics the number of times bloom filter has avoided file reads, i.e., negatives");

    snprintf(name, 255, "rdb.bf_point_positive_total@%s", str_gpid.c_str());
    _pfc_rdb_bf_point_positive_total.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_NUMBER,
        "statistics the number of times bloom FullFilter has not avoided the reads");

    snprintf(name, 255, "rdb.bf_point_negatives@%s", str_gpid.c_str());
    _pfc_rdb_bf_point_negatives.init_app_counter("app.pegasus",
                                                 name,
                                                 COUNTER_TYPE_NUMBER,
                                                 "statistics the number of times bloom FullFilter "
                                                 "has not avoided the reads and data actually "
                                                 "exist");

    auto counter_str = fmt::format("recent.read.throttling.reject.count@{}", str_gpid.c_str());
    _counter_recent_read_throttling_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());
}
} // namespace server
} // namespace pegasus
