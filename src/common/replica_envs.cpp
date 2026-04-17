// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "common/replica_envs.h"

#include <set>

namespace dsn {
const uint64_t replica_envs::MIN_SLOW_QUERY_THRESHOLD_MS = 20;
const std::string replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE("force");
const std::string replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP("skip");
const std::string replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_NORMAL("normal");
const std::string replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE("prefer_write");
const std::string replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD("bulk_load");
const std::string replica_envs::DENY_CLIENT_REQUEST("replica.deny_client_request");
const std::string replica_envs::WRITE_QPS_THROTTLING("replica.write_throttling");
const std::string replica_envs::WRITE_SIZE_THROTTLING("replica.write_throttling_by_size");
const std::string replica_envs::SLOW_QUERY_THRESHOLD("replica.slow_query_threshold");
const std::string replica_envs::ROCKSDB_USAGE_SCENARIO("rocksdb.usage_scenario");
/// default ttl for items in a table. If ttl is not set for
///   * a new written item, 'default_ttl' will be applied on this item.
///   * an exist item, 'default_ttl' will be applied on this item when it was compacted.
/// <= 0 means no effect
const std::string replica_envs::TABLE_LEVEL_DEFAULT_TTL("default_ttl");

/// A task of manual compaction can be triggered by update of app environment variables as follows:
/// Periodic manual compaction: triggered every day at the given `trigger_time`.
/// ```
/// manual_compact.periodic.trigger_time=3:00,21:00             // required
/// manual_compact.periodic.target_level=-1                     // optional, default -1
/// manual_compact.periodic.bottommost_level_compaction=force   // optional, default force
/// ```
///
/// Executed-once manual compaction: Triggered only at the specified unix time.
/// ```
/// manual_compact.once.trigger_time=1525930272                 // required
/// manual_compact.once.target_level=-1                         // optional, default -1
/// manual_compact.once.bottommost_level_compaction=force       // optional, default force
/// ```
///
/// Disable manual compaction:
/// ```
/// manual_compact.disabled=false                               // optional, default false
/// ```
const std::string MANUAL_COMPACT_PREFIX("manual_compact.");
const std::string replica_envs::MANUAL_COMPACT_DISABLED(MANUAL_COMPACT_PREFIX + "disabled");
const std::string replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT(
    MANUAL_COMPACT_PREFIX + "max_concurrent_running_count");
const std::string replica_envs::MANUAL_COMPACT_ONCE_PREFIX(MANUAL_COMPACT_PREFIX + "once.");
const std::string replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME(MANUAL_COMPACT_ONCE_PREFIX +
                                                                 "trigger_time");
// see more about the following two keys in rocksdb::CompactRangeOptions
const std::string replica_envs::MANUAL_COMPACT_TARGET_LEVEL("target_level");
const std::string replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL(MANUAL_COMPACT_ONCE_PREFIX +
                                                                 "target_level");
const std::string
    replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION("bottommost_level_compaction");
const std::string replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION(
    MANUAL_COMPACT_ONCE_PREFIX + "bottommost_level_compaction");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_PREFIX(MANUAL_COMPACT_PREFIX + "periodic.");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME(
    MANUAL_COMPACT_PERIODIC_PREFIX + "trigger_time");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL(
    MANUAL_COMPACT_PERIODIC_PREFIX + "target_level");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION(
    MANUAL_COMPACT_PERIODIC_PREFIX + "bottommost_level_compaction");
const std::string
    replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT("rocksdb.checkpoint.reserve_min_count");
const std::string replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS(
    "rocksdb.checkpoint.reserve_time_seconds");

/// time threshold of each rocksdb iteration
const std::string replica_envs::ROCKSDB_ITERATION_THRESHOLD_TIME_MS(
    "replica.rocksdb_iteration_threshold_time_ms");
const std::string replica_envs::ROCKSDB_BLOCK_CACHE_ENABLED("replica.rocksdb_block_cache_enabled");
const std::string replica_envs::BUSINESS_INFO("business.info");
const std::string replica_envs::REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS(
    "replica_access_controller.allowed_users");
const std::string replica_envs::REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES(
    "replica_access_controller.ranger_policies");
const std::string replica_envs::READ_QPS_THROTTLING("replica.read_throttling");
const std::string replica_envs::READ_SIZE_THROTTLING("replica.read_throttling_by_size");

/// true means compaction and scan will validate partition_hash, otherwise false
const std::string
    replica_envs::SPLIT_VALIDATE_PARTITION_HASH("replica.split.validate_partition_hash");

/// json string which represents user specified compaction
const std::string replica_envs::USER_SPECIFIED_COMPACTION("user_specified_compaction");
const std::string replica_envs::BACKUP_REQUEST_QPS_THROTTLING("replica.backup_request_throttling");
const std::string replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND("rocksdb.allow_ingest_behind");
const std::string replica_envs::UPDATE_MAX_REPLICA_COUNT("max_replica_count.update");
const std::string replica_envs::ROCKSDB_WRITE_BUFFER_SIZE("rocksdb.write_buffer_size");
const std::string replica_envs::ROCKSDB_NUM_LEVELS("rocksdb.num_levels");

// RocksDB dynamic options for SetOptions()
const std::string replica_envs::ROCKSDB_ENABLE_BLOB_FILES("rocksdb.enable_blob_files");
const std::string
    replica_envs::ROCKSDB_BLOB_FILE_STARTING_LEVEL("rocksdb.blob_file_starting_level");
const std::string replica_envs::ROCKSDB_MIN_BLOB_SIZE("rocksdb.min_blob_size");
const std::string replica_envs::ROCKSDB_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD(
    "rocksdb.blob_garbage_collection_force_threshold");
const std::string replica_envs::ROCKSDB_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF(
    "rocksdb.blob_garbage_collection_age_cutoff");
const std::string replica_envs::ROCKSDB_BLOB_FILE_SIZE("rocksdb.blob_file_size");
const std::string replica_envs::ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER(
    "rocksdb.level0_file_num_compaction_trigger");
const std::string
    replica_envs::ROCKSDB_MAX_BYTES_FOR_LEVEL_MULTIPLIER("rocksdb.max_bytes_for_level_multiplier");
const std::string
    replica_envs::ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE("rocksdb.max_bytes_for_level_base");
const std::string replica_envs::ROCKSDB_TARGET_FILE_SIZE_BASE("rocksdb.target_file_size_base");
const std::string replica_envs::ROCKSDB_MAX_WRITE_BUFFER_NUMBER("rocksdb.max_write_buffer_number");
const std::string
    replica_envs::ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER("rocksdb.level0_slowdown_writes_trigger");
const std::string
    replica_envs::ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER("rocksdb.level0_stop_writes_trigger");
const std::string replica_envs::ROCKSDB_ARENA_BLOCK_SIZE("rocksdb.arena_block_size");
// Memtable related options
const std::string
    replica_envs::ROCKSDB_INPLACE_UPDATE_NUM_LOCKS("rocksdb.inplace_update_num_locks");
const std::string replica_envs::ROCKSDB_MEMTABLE_PREFIX_BLOOM_SIZE_RATIO(
    "rocksdb.memtable_prefix_bloom_size_ratio");
const std::string
    replica_envs::ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING("rocksdb.memtable_whole_key_filtering");
const std::string replica_envs::ROCKSDB_MEMTABLE_HUGE_PAGE_SIZE("rocksdb.memtable_huge_page_size");
// Compaction related options
const std::string
    replica_envs::ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER("rocksdb.target_file_size_multiplier");
const std::string replica_envs::ROCKSDB_MAX_COMPACTION_BYTES("rocksdb.max_compaction_bytes");
const std::string replica_envs::ROCKSDB_IGNORE_MAX_COMPACTION_BYTES_FOR_INPUT(
    "rocksdb.ignore_max_compaction_bytes_for_input");
const std::string replica_envs::ROCKSDB_SOFT_PENDING_COMPACTION_BYTES_LIMIT(
    "rocksdb.soft_pending_compaction_bytes_limit");
const std::string replica_envs::ROCKSDB_HARD_PENDING_COMPACTION_BYTES_LIMIT(
    "rocksdb.hard_pending_compaction_bytes_limit");
// Blob file related options (additional)
const std::string replica_envs::ROCKSDB_BLOB_COMPRESSION_TYPE("rocksdb.blob_compression_type");
const std::string
    replica_envs::ROCKSDB_ENABLE_BLOB_GARBAGE_COLLECTION("rocksdb.enable_blob_garbage_collection");
const std::string
    replica_envs::ROCKSDB_BLOB_COMPACTION_READAHEAD_SIZE("rocksdb.blob_compaction_readahead_size");
const std::string replica_envs::ROCKSDB_PREPOPULATE_BLOB_CACHE("rocksdb.prepopulate_blob_cache");
// Other options
const std::string
    replica_envs::ROCKSDB_PERIODIC_COMPACTION_SECONDS("rocksdb.periodic_compaction_seconds");
const std::string replica_envs::ROCKSDB_BOTTOMMOST_TEMPERATURE("rocksdb.bottommost_temperature");
const std::string replica_envs::ROCKSDB_LAST_LEVEL_TEMPERATURE("rocksdb.last_level_temperature");

const std::set<std::string> replica_envs::ROCKSDB_DYNAMIC_OPTIONS = {
    replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
    replica_envs::ROCKSDB_ENABLE_BLOB_FILES,
    replica_envs::ROCKSDB_BLOB_FILE_STARTING_LEVEL,
    replica_envs::ROCKSDB_MIN_BLOB_SIZE,
    replica_envs::ROCKSDB_BLOB_GARBAGE_COLLECTION_FORCE_THRESHOLD,
    replica_envs::ROCKSDB_BLOB_GARBAGE_COLLECTION_AGE_CUTOFF,
    replica_envs::ROCKSDB_BLOB_FILE_SIZE,
    replica_envs::ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER,
    replica_envs::ROCKSDB_MAX_BYTES_FOR_LEVEL_MULTIPLIER,
    replica_envs::ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE,
    replica_envs::ROCKSDB_TARGET_FILE_SIZE_BASE,
    replica_envs::ROCKSDB_MAX_WRITE_BUFFER_NUMBER,
    replica_envs::ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER,
    replica_envs::ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER,
    replica_envs::ROCKSDB_ARENA_BLOCK_SIZE,
    // Memtable related options
    replica_envs::ROCKSDB_INPLACE_UPDATE_NUM_LOCKS,
    replica_envs::ROCKSDB_MEMTABLE_PREFIX_BLOOM_SIZE_RATIO,
    replica_envs::ROCKSDB_MEMTABLE_WHOLE_KEY_FILTERING,
    replica_envs::ROCKSDB_MEMTABLE_HUGE_PAGE_SIZE,
    // Compaction related options
    replica_envs::ROCKSDB_TARGET_FILE_SIZE_MULTIPLIER,
    replica_envs::ROCKSDB_MAX_COMPACTION_BYTES,
    replica_envs::ROCKSDB_IGNORE_MAX_COMPACTION_BYTES_FOR_INPUT,
    replica_envs::ROCKSDB_SOFT_PENDING_COMPACTION_BYTES_LIMIT,
    replica_envs::ROCKSDB_HARD_PENDING_COMPACTION_BYTES_LIMIT,
    // Blob file related options (additional)
    replica_envs::ROCKSDB_BLOB_COMPRESSION_TYPE,
    replica_envs::ROCKSDB_ENABLE_BLOB_GARBAGE_COLLECTION,
    replica_envs::ROCKSDB_BLOB_COMPACTION_READAHEAD_SIZE,
    replica_envs::ROCKSDB_PREPOPULATE_BLOB_CACHE,
    // Other options
    replica_envs::ROCKSDB_PERIODIC_COMPACTION_SECONDS,
    replica_envs::ROCKSDB_BOTTOMMOST_TEMPERATURE,
    replica_envs::ROCKSDB_LAST_LEVEL_TEMPERATURE,
};
const std::set<std::string> replica_envs::ROCKSDB_STATIC_OPTIONS = {
    replica_envs::ROCKSDB_NUM_LEVELS,
};
} // namespace dsn
