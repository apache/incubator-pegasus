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

#pragma once

#ifdef DSN_USE_JEMALLOC

#include <string>

#include <dsn/utility/enum_helper.h>

namespace dsn {

enum class je_stats_type : size_t
{
    SUMMARY_STATS,
    CONFIGS,
    BRIEF_ARENA_STATS,
    DETAILED_STATS,
    COUNT,
    INVALID,
};

ENUM_BEGIN(je_stats_type, je_stats_type::INVALID)
ENUM_REG2(je_stats_type, SUMMARY_STATS)
ENUM_REG2(je_stats_type, CONFIGS)
ENUM_REG2(je_stats_type, BRIEF_ARENA_STATS)
ENUM_REG2(je_stats_type, DETAILED_STATS)
ENUM_END(je_stats_type)

std::string get_all_je_stats_types_str();
const std::string kAllJeStatsTypesStr = get_all_je_stats_types_str();

void je_dump_stats(je_stats_type type, size_t buf_sz, std::string &stats);

void je_dump_stats(je_stats_type type, std::string &stats);

} // namespace dsn

#endif // DSN_USE_JEMALLOC
