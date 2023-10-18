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

#include "pegasus_const.h"

#include <rocksdb/options.h>
#include <stddef.h>
#include <stdint.h>

#include "utils/string_conv.h"

namespace pegasus {

// should be same with items in dsn::backup_restore_constant
const std::string ROCKSDB_ENV_RESTORE_FORCE_RESTORE("restore.force_restore");
const std::string ROCKSDB_ENV_RESTORE_POLICY_NAME("restore.policy_name");
const std::string ROCKSDB_ENV_RESTORE_BACKUP_ID("restore.backup_id");

const std::string ROCKSDB_ENV_USAGE_SCENARIO_KEY("rocksdb.usage_scenario");
const std::string ROCKSDB_ENV_USAGE_SCENARIO_NORMAL("normal");
const std::string ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE("prefer_write");
const std::string ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD("bulk_load");

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
const std::string MANUAL_COMPACT_KEY_PREFIX("manual_compact.");
const std::string MANUAL_COMPACT_DISABLED_KEY(MANUAL_COMPACT_KEY_PREFIX + "disabled");
const std::string MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT_KEY(MANUAL_COMPACT_KEY_PREFIX +
                                                                  "max_concurrent_running_count");

const std::string MANUAL_COMPACT_PERIODIC_KEY_PREFIX(MANUAL_COMPACT_KEY_PREFIX + "periodic.");
const std::string MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY(MANUAL_COMPACT_PERIODIC_KEY_PREFIX +
                                                           "trigger_time");

const std::string MANUAL_COMPACT_ONCE_KEY_PREFIX(MANUAL_COMPACT_KEY_PREFIX + "once.");
const std::string MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY(MANUAL_COMPACT_ONCE_KEY_PREFIX +
                                                       "trigger_time");

// see more about the following two keys in rocksdb::CompactRangeOptions
const std::string MANUAL_COMPACT_TARGET_LEVEL_KEY("target_level");

const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY("bottommost_level_compaction");
const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE("force");
const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP("skip");

/// default ttl for items in a table. If ttl is not set for
///   * a new written item, 'default_ttl' will be applied on this item.
///   * an exist item, 'default_ttl' will be applied on this item when it was compacted.
/// <= 0 means no effect
const std::string TABLE_LEVEL_DEFAULT_TTL("default_ttl");

const std::string ROCKDB_CHECKPOINT_RESERVE_MIN_COUNT("rocksdb.checkpoint.reserve_min_count");
const std::string ROCKDB_CHECKPOINT_RESERVE_TIME_SECONDS("rocksdb.checkpoint.reserve_time_seconds");

/// read cluster meta address from this section
const std::string PEGASUS_CLUSTER_SECTION_NAME("pegasus.clusters");

/// table level slow query
const std::string ROCKSDB_ENV_SLOW_QUERY_THRESHOLD("replica.slow_query_threshold");

/// enable or disable block cache of app
const std::string ROCKSDB_BLOCK_CACHE_ENABLED("replica.rocksdb_block_cache_enabled");

/// time threshold of each rocksdb iteration
const std::string
    ROCKSDB_ITERATION_THRESHOLD_TIME_MS("replica.rocksdb_iteration_threshold_time_ms");

/// true means compaction and scan will validate partition_hash, otherwise false
const std::string SPLIT_VALIDATE_PARTITION_HASH("replica.split.validate_partition_hash");

/// json string which represents user specified compaction
const std::string USER_SPECIFIED_COMPACTION("user_specified_compaction");

const std::string READ_SIZE_THROTTLING("replica.read_throttling_by_size");

const std::string ROCKSDB_ALLOW_INGEST_BEHIND("rocksdb.allow_ingest_behind");

const std::string ROCKSDB_WRITE_BUFFER_SIZE("rocksdb.write_buffer_size");

const std::string ROCKSDB_NUM_LEVELS("rocksdb.num_levels");

const std::set<std::string> ROCKSDB_DYNAMIC_OPTIONS = {
    ROCKSDB_WRITE_BUFFER_SIZE,
};
const std::set<std::string> ROCKSDB_STATIC_OPTIONS = {
    ROCKSDB_NUM_LEVELS,
};

const std::unordered_map<std::string, cf_opts_setter> cf_opts_setters = {
    {ROCKSDB_WRITE_BUFFER_SIZE,
     [](const std::string &str, rocksdb::ColumnFamilyOptions &option) -> bool {
         uint64_t val = 0;
         if (!dsn::buf2uint64(str, val)) {
             return false;
         }
         option.write_buffer_size = static_cast<size_t>(val);
         return true;
     }},
    {ROCKSDB_NUM_LEVELS,
     [](const std::string &str, rocksdb::ColumnFamilyOptions &option) -> bool {
         int32_t val = 0;
         if (!dsn::buf2int32(str, val)) {
             return false;
         }
         option.num_levels = val;
         return true;
     }},
};

const std::unordered_map<std::string, cf_opts_getter> cf_opts_getters = {
    {ROCKSDB_WRITE_BUFFER_SIZE,
     [](const rocksdb::ColumnFamilyOptions &option, /*out*/ std::string &str) {
         str = std::to_string(option.write_buffer_size);
     }},
    {ROCKSDB_NUM_LEVELS,
     [](const rocksdb::ColumnFamilyOptions &option, /*out*/ std::string &str) {
         str = std::to_string(option.num_levels);
     }},
};
} // namespace pegasus
