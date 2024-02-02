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

#include "common/replica_envs.h"
#include "utils/string_conv.h"

namespace pegasus {

const std::string ROCKSDB_ENV_USAGE_SCENARIO_KEY("rocksdb.usage_scenario");
const std::string ROCKSDB_ENV_USAGE_SCENARIO_NORMAL("normal");
const std::string ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE("prefer_write");
const std::string ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD("bulk_load");

const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE("force");
const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP("skip");

/// read cluster meta address from this section
const std::string PEGASUS_CLUSTER_SECTION_NAME("pegasus.clusters");

const std::unordered_map<std::string, cf_opts_setter> cf_opts_setters = {
    {dsn::replication::replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
     [](const std::string &str, rocksdb::ColumnFamilyOptions &option) -> bool {
         uint64_t val = 0;
         if (!dsn::buf2uint64(str, val)) {
             return false;
         }
         option.write_buffer_size = static_cast<size_t>(val);
         return true;
     }},
    {dsn::replication::replica_envs::ROCKSDB_NUM_LEVELS,
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
    {dsn::replication::replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
     [](const rocksdb::ColumnFamilyOptions &option, /*out*/ std::string &str) {
         str = std::to_string(option.write_buffer_size);
     }},
    {dsn::replication::replica_envs::ROCKSDB_NUM_LEVELS,
     [](const rocksdb::ColumnFamilyOptions &option, /*out*/ std::string &str) {
         str = std::to_string(option.num_levels);
     }},
};
} // namespace pegasus
