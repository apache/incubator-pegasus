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

const std::set<std::string> replica_envs::ROCKSDB_DYNAMIC_OPTIONS = {
    replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
};
const std::set<std::string> replica_envs::ROCKSDB_STATIC_OPTIONS = {
    replica_envs::ROCKSDB_NUM_LEVELS,
};
} // namespace dsn
