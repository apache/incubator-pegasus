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

#include "utils/throttling_controller.h"

#include "gtest/gtest.h"

namespace dsn {
namespace utils {

class throttling_controller_test : public ::testing::Test
{
public:
    void test_parse_env_basic()
    {
        utils::throttling_controller cntl;
        std::string parse_err;
        bool env_changed = false;
        std::string old_value;
        ASSERT_TRUE(cntl.parse_from_env("20000*delay*100", 4, parse_err, env_changed, old_value));
        ASSERT_EQ(cntl._cur_units, 0);
        ASSERT_EQ(cntl._enabled, true);
        ASSERT_EQ(cntl._delay_ms, 100);
        ASSERT_EQ(cntl._delay_units, 5000 + 1);
        ASSERT_EQ(cntl._reject_delay_ms, 0);
        ASSERT_EQ(cntl._reject_units, 0);
        ASSERT_EQ(cntl._env_value, "20000*delay*100");
        ASSERT_EQ(cntl._partition_count, 4);
        ASSERT_EQ(env_changed, true);
        ASSERT_EQ(old_value, "");
        ASSERT_EQ(parse_err, "");

        ASSERT_TRUE(cntl.parse_from_env(
            "20000*delay*100,20000*reject*100", 4, parse_err, env_changed, old_value));
        ASSERT_EQ(cntl._cur_units, 0);
        ASSERT_EQ(cntl._enabled, true);
        ASSERT_EQ(cntl._delay_ms, 100);
        ASSERT_EQ(cntl._delay_units, 5000 + 1);
        ASSERT_EQ(cntl._reject_delay_ms, 100);
        ASSERT_EQ(cntl._reject_units, 5000 + 1);
        ASSERT_EQ(cntl._env_value, "20000*delay*100,20000*reject*100");
        ASSERT_EQ(cntl._partition_count, 4);
        ASSERT_EQ(env_changed, true);
        ASSERT_EQ(old_value, "20000*delay*100");
        ASSERT_EQ(parse_err, "");

        // invalid argument

        ASSERT_FALSE(cntl.parse_from_env("*delay*100", 4, parse_err, env_changed, old_value));
        ASSERT_EQ(env_changed, false);
        ASSERT_NE(parse_err, "");
        ASSERT_EQ(cntl._enabled, true); // ensure invalid env won't stop throttling
        ASSERT_EQ(cntl._delay_ms, 100);
        ASSERT_EQ(cntl._delay_units, 5000 + 1);
        ASSERT_EQ(cntl._reject_delay_ms, 100);
        ASSERT_EQ(cntl._reject_units, 5000 + 1);

        ASSERT_FALSE(cntl.parse_from_env("", 4, parse_err, env_changed, old_value));
        ASSERT_EQ(env_changed, false);
        ASSERT_NE(parse_err, "");
        ASSERT_EQ(cntl._enabled, true);
    }

    void test_parse_env_multiplier()
    {
        utils::throttling_controller cntl;
        std::string parse_err;
        bool env_changed = false;
        std::string old_value;

        struct test_case_1
        {
            std::string env;
            int64_t delay_units;
            int64_t delay_ms;
            int64_t reject_units;
            int64_t reject_ms;
        } test_cases_1[] = {
            {"20K*delay*100", (5 << 10) + 1, 100, 0, 0},
            {"20M*delay*100", (5 << 20) + 1, 100, 0, 0},
            {"20M*delay*100,20M*reject*100", (5 << 20) + 1, 100, (5 << 20) + 1, 100},
            // throttling size exceeds int32_t max value
            {"80000M*delay*100", (20000ULL << 20) + 1, 100, 0, 0},
        };
        for (const auto &tc : test_cases_1) {
            ASSERT_TRUE(cntl.parse_from_env(tc.env, 4, parse_err, env_changed, old_value));
            ASSERT_EQ(cntl._enabled, true);
            ASSERT_EQ(cntl._delay_units, tc.delay_units) << tc.env;
            ASSERT_EQ(cntl._delay_ms, tc.delay_ms) << tc.env;
            ASSERT_EQ(cntl._reject_units, tc.reject_units) << tc.env;
            ASSERT_EQ(cntl._reject_delay_ms, tc.reject_ms) << tc.env;
            ASSERT_EQ(env_changed, true);
            ASSERT_EQ(parse_err, "");
        }

        // invalid argument

        std::string test_cases_2[] = {
            "20m*delay*100",
            "20B*delay*100",
            "20KB*delay*100",
            "20Mb*delay*100",
            "20MB*delay*100",
        };
        for (const std::string &tc : test_cases_2) {
            ASSERT_FALSE(cntl.parse_from_env(tc, 4, parse_err, env_changed, old_value));
            ASSERT_EQ(cntl._enabled, true);
            ASSERT_EQ(env_changed, false);
            ASSERT_NE(parse_err, "");
        }
    }
};

TEST_F(throttling_controller_test, parse_env_basic) { test_parse_env_basic(); }

TEST_F(throttling_controller_test, parse_env_multiplier) { test_parse_env_multiplier(); }

} // namespace utils
} // namespace dsn
