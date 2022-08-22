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

#ifdef DSN_USE_JEMALLOC

#include "utils/je_ctl.h"

#include <gtest/gtest.h>

namespace dsn {

namespace {

// This function does not check "End" mark, since sometimes returned message is very long while
// the buffer may not have enough space: the message will be truncated. If it is known that the
// message can be held by the buffer completely, `check_base_stats_marks_with_end` should be used
// instead.
void check_base_stats_marks(const std::string &stats)
{
    ASSERT_NE(stats.find("Begin jemalloc statistics"), std::string::npos);
    ASSERT_NE(stats.find("Allocated:"), std::string::npos);
    ASSERT_NE(stats.find("Background threads:"), std::string::npos);
}

void check_base_stats_marks_with_end(const std::string &stats)
{
    check_base_stats_marks(stats);
    ASSERT_NE(stats.find("End jemalloc statistics"), std::string::npos);
}

void check_configs_marks(const std::string &stats)
{
    ASSERT_NE(stats.find("Version:"), std::string::npos);
    ASSERT_NE(stats.find("Build-time option settings"), std::string::npos);
    ASSERT_NE(stats.find("Run-time option settings"), std::string::npos);
    ASSERT_NE(stats.find("Profiling settings"), std::string::npos);
}

void check_arena_marks(const std::string &stats)
{
    // Marks for merged arenas.
    ASSERT_NE(stats.find("Merged arenas stats:"), std::string::npos);

    // Marks for each arena.
    ASSERT_NE(stats.find("arenas[0]:"), std::string::npos);
}

} // anonymous namespace

TEST(je_ctl_test, dump_summary_stats)
{
    std::string stats;
    je_dump_stats(je_stats_type::SUMMARY_STATS, stats);

    check_base_stats_marks_with_end(stats);
}

TEST(je_ctl_test, dump_configs)
{
    std::string stats;
    je_dump_stats(je_stats_type::CONFIGS, stats);

    check_base_stats_marks_with_end(stats);
    check_configs_marks(stats);
}

TEST(je_ctl_test, dump_brief_arena_stats)
{
    std::string stats;
    je_dump_stats(je_stats_type::BRIEF_ARENA_STATS, stats);

    // Since there may be many arenas, "End" mark is not required to be checked here.
    check_base_stats_marks(stats);
    check_arena_marks(stats);
}

TEST(je_ctl_test, dump_detailed_stats)
{
    std::string stats;
    je_dump_stats(je_stats_type::DETAILED_STATS, stats);

    // Since there may be many arenas, "End" mark is not required to be checked here.
    check_base_stats_marks(stats);

    // Detailed stats will contain all information, therefore everything should be checked.
    check_configs_marks(stats);
    check_arena_marks(stats);
    ASSERT_NE(stats.find("bins:"), std::string::npos);
    ASSERT_NE(stats.find("extents:"), std::string::npos);
}

} // namespace dsn

#endif // DSN_USE_JEMALLOC
