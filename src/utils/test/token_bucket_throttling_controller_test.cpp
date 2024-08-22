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

#include <fmt/core.h>
#include <stdint.h>
#include <unistd.h>
#include <limits>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "utils/TokenBucket.h"
#include "utils/test_macros.h"
#include "utils/throttling_controller.h"
#include "utils/token_bucket_throttling_controller.h"

namespace dsn {
namespace utils {

class token_bucket_throttling_controller_test : public ::testing::Test
{
public:
    void INVALIDATE_SITUATION_CHECK(const std::string &env)
    {
        std::string old_value, parse_err;
        bool env_changed_result = false;
        ASSERT_FALSE(cntl.parse_from_env(env, 4, parse_err, env_changed_result, old_value));
        ASSERT_EQ(env_changed_result, false);
        ASSERT_NE(parse_err, "");
        ASSERT_EQ(cntl._enabled, true);
        ASSERT_EQ(old_value, old_env);
        ASSERT_EQ(cntl._env_value, old_env);
    }

    void VALIDATE_SITUATION_CHECK(const std::string &env,
                                  int partition_count,
                                  uint64_t throttle_size,
                                  bool enabled,
                                  bool env_changed,
                                  const std::string &old_env)
    {
        bool env_changed_result = false;
        std::string old_value, parse_err;
        int32_t partitioned_throttle_size = throttle_size / partition_count;
        ASSERT_TRUE(
            cntl.parse_from_env(env, partition_count, parse_err, env_changed_result, old_value));
        ASSERT_EQ(cntl._env_value, env);
        ASSERT_EQ(cntl._partition_count, partition_count);
        ASSERT_EQ(cntl._burstsize, partitioned_throttle_size);
        ASSERT_EQ(cntl._rate, partitioned_throttle_size);
        ASSERT_EQ(cntl._enabled, enabled);
        ASSERT_EQ(env_changed_result, env_changed);
        ASSERT_EQ(old_value, old_env);
        ASSERT_EQ(parse_err, "");
    }

    void test_parse_env_basic_token_bucket_throttling()
    {
        // token_bucket_throttling_controller doesn't support delay only
        NO_FATALS(VALIDATE_SITUATION_CHECK("20000*delay*100", 4, 0, false, true, ""));
        NO_FATALS(VALIDATE_SITUATION_CHECK("200K", 4, 200 << 10, true, true, "20000*delay*100"));
        NO_FATALS(VALIDATE_SITUATION_CHECK(
            "20000*delay*100,20000*reject*100", 4, 20000, true, true, "200K"));
        NO_FATALS(VALIDATE_SITUATION_CHECK("20K*delay*100,20K*reject*100",
                                           4,
                                           20 << 10,
                                           true,
                                           true,
                                           "20000*delay*100,20000*reject*100"));
        NO_FATALS(VALIDATE_SITUATION_CHECK(
            "20000*reject*100", 4, 20000, true, true, "20K*delay*100,20K*reject*100"));

        // invalid argument]
        NO_FATALS(INVALIDATE_SITUATION_CHECK("0"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("*deldday*100"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK(""));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("*reject"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("*reject*"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("reject*"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("reject"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("200g"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("200G"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("M"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("K"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("-1K"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("1aK"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("pegNo1"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("-20"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("12KM"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("1K2M"));
        NO_FATALS(INVALIDATE_SITUATION_CHECK("2000K0*reject*100"));
    }

    void throttle_test()
    {
        auto cntl = std::make_unique<token_bucket_throttling_controller>();
        std::string parse_err;
        bool env_changed = false;
        std::string old_value;
        const int partition_count = 4;

        int throttle_limit = 200000;
        cntl->parse_from_env(
            std::to_string(throttle_limit), partition_count, parse_err, env_changed, old_value);

        auto token_bucket = std::make_unique<DynamicTokenBucket>();
        int fail_count = 0;
        for (int i = 0; i < 100000; i++) {
            token_bucket->consumeWithBorrowAndWait(
                1, throttle_limit / partition_count * 0.8, throttle_limit / partition_count * 1.0);
            cntl->consume_token(1);
            if (!cntl->available()) {
                fail_count++;
            }
        }
        ASSERT_EQ(fail_count, 0);

        sleep(1);

        fail_count = 0;
        for (int i = 0; i < 100000; i++) {
            token_bucket->consumeWithBorrowAndWait(
                1, throttle_limit / partition_count * 1.2, throttle_limit / partition_count * 1.5);
            cntl->consume_token(1);
            if (!cntl->available()) {
                fail_count++;
            }
        }
        ASSERT_GT(fail_count, 10000);

        sleep(1);

        fail_count = 0;
        int fail_count1 = 0;
        for (int i = 0; i < 200000; i++) {
            if (i < 100000) {
                token_bucket->consumeWithBorrowAndWait(1,
                                                       throttle_limit / partition_count * 1.2,
                                                       throttle_limit / partition_count * 1.5);
                fail_count1 = fail_count;
            } else {
                token_bucket->consumeWithBorrowAndWait(1,
                                                       throttle_limit / partition_count * 0.2,
                                                       throttle_limit / partition_count * 0.3);
            }
            if (!cntl->consume_token(1)) {
                fail_count++;
            }
        }
        ASSERT_GT(fail_count1, 10000);
        ASSERT_LE(fail_count, fail_count1 * 1.2);
    }

private:
    token_bucket_throttling_controller cntl;

    static const std::string old_env;
};

const std::string token_bucket_throttling_controller_test::old_env = "20000*reject*100";

TEST_F(token_bucket_throttling_controller_test, test_parse_env_basic_token_bucket_throttling)
{
    test_parse_env_basic_token_bucket_throttling();
}

TEST_F(token_bucket_throttling_controller_test, throttle_test) { throttle_test(); }

TEST_F(token_bucket_throttling_controller_test, parse_unit_test)
{
    std::string max_minus_1 = std::to_string(std::numeric_limits<uint64_t>::max() - 1);
    struct parse_unit_test_case
    {
        std::string input;
        bool expected_result;
        uint64_t expected_units;
        std::string expected_hint_message_piece;
    } tests[] = {
        {"", false, 0, "integer"},
        {"-", false, 0, "integer"},
        {"A", false, 0, "integer"},
        {"M", false, 0, "integer"},
        {"K", false, 0, "integer"},
        {"aM", false, 0, "integer"},
        {"aK", false, 0, "integer"},
        {fmt::format("{}M", max_minus_1), false, 0, "overflow"},
        {fmt::format("{}K", max_minus_1), false, 0, "overflow"},
        {"10M", true, 10 << 20, ""},
        {"1K", true, 1 << 10, ""},
        {"100", true, 100, ""},
    };

    for (const auto &test : tests) {
        uint64_t actual_units = 0;
        std::string actual_hint_message;
        ASSERT_EQ(test.expected_result,
                  throttling_controller::parse_unit(test.input, actual_units, actual_hint_message));
        ASSERT_EQ(test.expected_units, actual_units);
        if (test.expected_result) {
            ASSERT_EQ(actual_hint_message, "");
        } else {
            ASSERT_STR_CONTAINS(actual_hint_message, test.expected_hint_message_piece);
        }
    }
};
} // namespace utils
} // namespace dsn
