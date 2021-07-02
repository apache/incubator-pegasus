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

extern const std::string ROCKSDB_ENV_SLOW_QUERY_THRESHOLD;

extern const std::string ROCKSDB_ITERATION_THRESHOLD_TIME_MS;

extern const std::string SPLIT_VALIDATE_PARTITION_HASH;

extern const std::string USER_SPECIFIED_COMPACTION;
} // namespace pegasus
