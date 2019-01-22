// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_const.h"

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
} // namespace pegasus
