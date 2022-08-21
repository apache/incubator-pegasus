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

void check_stats_marks(const std::string &stats)
{
    ASSERT_NE(stats.find("Begin jemalloc statistics"), std::string::npos);
    ASSERT_NE(stats.find("End jemalloc statistics"), std::string::npos);
}

} // anonymous namespace

TEST(je_ctl_test, dump_summary_stats)
{
    std::string stats;
    je_dump_stats(je_stats_type::SUMMARY_STATS, stats);

    check_stats_marks(stats);
}

TEST(je_ctl_test, dump_configs)
{
    std::string stats;
    je_dump_stats(je_stats_type::CONFIGS, stats);

    check_stats_marks(stats);
    ASSERT_NE(stats.find("Build-time option settings"), std::string::npos);
    ASSERT_NE(stats.find("Run-time option settings"), std::string::npos);
    ASSERT_NE(stats.find("Profiling settings"), std::string::npos);
}

TEST(je_ctl_test, dump_brief_arena_stats)
{
    std::string stats;
    je_dump_stats(je_stats_type::BRIEF_ARENA_STATS, stats);

    check_stats_marks(stats);
    ASSERT_NE(stats.find("arenas[0]:"), std::string::npos);
    ASSERT_NE(stats.find("assigned threads:"), std::string::npos);
    ASSERT_NE(stats.find("decaying:"), std::string::npos);
}

} // namespace dsn

#endif // DSN_USE_JEMALLOC
