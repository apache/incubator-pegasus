// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>

namespace pegasus {

const int SCAN_CONTEXT_ID_VALID_MIN = 0;
const int SCAN_CONTEXT_ID_COMPLETED = -1;
const int SCAN_CONTEXT_ID_NOT_EXIST = -2;

extern const std::string ROCKSDB_ENV_RESTORE_FORCE_RESTORE;
extern const std::string ROCKSDB_ENV_RESTORE_POLICY_NAME;
extern const std::string ROCKSDB_ENV_RESTORE_BACKUP_ID;

extern const std::string ROCKSDB_ENV_USAGE_SCENARIO_KEY;
extern const std::string ROCKSDB_ENV_USAGE_SCENARIO_NORMAL;
extern const std::string ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE;
extern const std::string ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD;

extern const std::string MANUAL_COMPACT_KEY_PREFIX;
extern const std::string MANUAL_COMPACT_DISABLED_KEY;
extern const std::string MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT_KEY;

extern const std::string MANUAL_COMPACT_PERIODIC_KEY_PREFIX;
extern const std::string MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY;

extern const std::string MANUAL_COMPACT_ONCE_KEY_PREFIX;
extern const std::string MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY;

extern const std::string MANUAL_COMPACT_TARGET_LEVEL_KEY;

extern const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY;
extern const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE;
extern const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP;

extern const std::string TABLE_LEVEL_DEFAULT_TTL;

extern const std::string ROCKDB_CHECKPOINT_RESERVE_MIN_COUNT;
extern const std::string ROCKDB_CHECKPOINT_RESERVE_TIME_SECONDS;

extern const std::string PEGASUS_CLUSTER_SECTION_NAME;
} // namespace pegasus
