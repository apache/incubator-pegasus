/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <cstdint>
#include <string>
#include <set>

namespace dsn {

class replica_envs
{
public:
    // Environment variable keys.
    static const std::string DENY_CLIENT_REQUEST;
    static const std::string WRITE_QPS_THROTTLING;
    static const std::string WRITE_SIZE_THROTTLING;
    static const std::string SLOW_QUERY_THRESHOLD;
    static const std::string TABLE_LEVEL_DEFAULT_TTL;
    static const std::string ROCKSDB_USAGE_SCENARIO;
    static const std::string ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT;
    static const std::string ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS;
    static const std::string ROCKSDB_ITERATION_THRESHOLD_TIME_MS;
    static const std::string ROCKSDB_BLOCK_CACHE_ENABLED;
    static const std::string MANUAL_COMPACT_ONCE_PREFIX;
    static const std::string MANUAL_COMPACT_PERIODIC_PREFIX;
    static const std::string MANUAL_COMPACT_DISABLED;
    static const std::string MANUAL_COMPACT_TARGET_LEVEL;
    static const std::string MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT;
    static const std::string MANUAL_COMPACT_ONCE_TRIGGER_TIME;
    static const std::string MANUAL_COMPACT_ONCE_TARGET_LEVEL;
    static const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION;
    static const std::string MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION;
    static const std::string MANUAL_COMPACT_PERIODIC_TRIGGER_TIME;
    static const std::string MANUAL_COMPACT_PERIODIC_TARGET_LEVEL;
    static const std::string MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION;
    static const std::string BUSINESS_INFO;
    static const std::string REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS;
    static const std::string REPLICA_ACCESS_CONTROLLER_RANGER_POLICIES;
    static const std::string READ_QPS_THROTTLING;
    static const std::string READ_SIZE_THROTTLING;
    static const std::string BACKUP_REQUEST_QPS_THROTTLING;
    static const std::string SPLIT_VALIDATE_PARTITION_HASH;
    static const std::string USER_SPECIFIED_COMPACTION;
    static const std::string ROCKSDB_ALLOW_INGEST_BEHIND;
    static const std::string UPDATE_MAX_REPLICA_COUNT;
    static const std::string ROCKSDB_WRITE_BUFFER_SIZE;
    static const std::string ROCKSDB_NUM_LEVELS;

    static const std::set<std::string> ROCKSDB_DYNAMIC_OPTIONS;
    static const std::set<std::string> ROCKSDB_STATIC_OPTIONS;

    // Environment variable values.
    static const uint64_t MIN_SLOW_QUERY_THRESHOLD_MS;
    static const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE;
    static const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP;
    static const std::string ROCKSDB_ENV_USAGE_SCENARIO_NORMAL;
    static const std::string ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE;
    static const std::string ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD;
};

} // namespace dsn
