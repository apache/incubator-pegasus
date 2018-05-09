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

const std::string
    ROCKSDB_MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY("bottommost_level_compaction");
const std::string ROCKSDB_MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE("force");
const std::string ROCKSDB_MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP("skip");

const std::string ROCKSDB_MANUAL_COMPACT_TARGET_LEVEL_KEY("target_level");

} // namespace
