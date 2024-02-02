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

#include <functional>
#include <set>
#include <string>
#include <unordered_map>

namespace rocksdb {
struct ColumnFamilyOptions;
} // namespace rocksdb

namespace pegasus {

const int SCAN_CONTEXT_ID_VALID_MIN = 0;
const int SCAN_CONTEXT_ID_COMPLETED = -1;
const int SCAN_CONTEXT_ID_NOT_EXIST = -2;

extern const std::string MANUAL_COMPACT_TARGET_LEVEL_KEY;

extern const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE;
extern const std::string MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP;

extern const std::string PEGASUS_CLUSTER_SECTION_NAME;

using cf_opts_setter = std::function<bool(const std::string &, rocksdb::ColumnFamilyOptions &)>;
extern const std::unordered_map<std::string, cf_opts_setter> cf_opts_setters;

using cf_opts_getter =
    std::function<void(const rocksdb::ColumnFamilyOptions &, /*out*/ std::string &)>;
extern const std::unordered_map<std::string, cf_opts_getter> cf_opts_getters;
} // namespace pegasus
