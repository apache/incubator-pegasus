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

#include "utils/token_bucket_throttling_controller.h"

#include <unistd.h>

#include "gtest/gtest.h"

namespace dsn {
namespace utils {

#define INVALIDATE_SITUATION_CHECK(env)                                                            \
    do {                                                                                           \
        std::string old_value, parse_err;                                                          \
        bool env_changed_result = false;                                                           \
        ASSERT_FALSE(cntl.parse_from_env(env, 4, parse_err, env_changed_result, old_value));       \
        ASSERT_EQ(env_changed_result, false);                                                      \
        ASSERT_EQ(parse_err, "wrong format, you can set like 20000 or 20K");                       \
        ASSERT_EQ(cntl._enabled, true);                                                            \
        ASSERT_EQ(old_value, old_env);                                                             \
        ASSERT_EQ(cntl._env_value, old_env);                                                       \
    } while (0)

#define VALIDATE_SITUATION_CHECK(                                                                  \
    env, partition_count, throttle_size, enabled, env_changed, old_env)                            \
    do {                                                                                           \
        bool env_changed_result = false;                                                           \
        std::string old_value, parse_err;                                                          \
        int32_t partitioned_throttle_size = throttle_size / partition_count;                       \
        ASSERT_TRUE(                                                                               \
            cntl.parse_from_env(env, partition_count, parse_err, env_changed_result, old_value));  \
        ASSERT_EQ(cntl._env_value, env);                                                           \
        ASSERT_EQ(cntl._partition_count, partition_count);                                         \
        ASSERT_EQ(cntl._burstsize, partitioned_throttle_size);                                     \
        ASSERT_EQ(cntl._rate, partitioned_throttle_size);                                          \
        ASSERT_EQ(cntl._enabled, enabled);                                                         \
        ASSERT_EQ(env_changed_result, env_changed);                                                \
        ASSERT_EQ(old_value, old_env);                                                             \
        ASSERT_EQ(parse_err, "");                                                                  \
    } while (0)

class token_bucket_throttling_controller_test : public ::testing::Test
{
public:
    void test_parse_env_basic_token_bucket_throttling()
    {
        token_bucket_throttling_controller cntl;

        // token_bucket_throttling_controller doesn't support delay only
        VALIDATE_SITUATION_CHECK("20000*delay*100", 4, 0, false, true, "");
        VALIDATE_SITUATION_CHECK("200K", 4, 200000, true, true, "20000*delay*100");
        VALIDATE_SITUATION_CHECK("20000*delay*100,20000*reject*100", 4, 20000, true, true, "200K");
        VALIDATE_SITUATION_CHECK("20K*delay*100,20K*reject*100",
                                 4,
                                 20000,
                                 true,
                                 true,
                                 "20000*delay*100,20000*reject*100");
        VALIDATE_SITUATION_CHECK(
            "20000*reject*100", 4, 20000, true, true, "20K*delay*100,20K*reject*100");

        // invalid argument]
        std::string old_env = "20000*reject*100";
        INVALIDATE_SITUATION_CHECK("0");
        INVALIDATE_SITUATION_CHECK("*deldday*100");
        INVALIDATE_SITUATION_CHECK("");
        INVALIDATE_SITUATION_CHECK("*reject");
        INVALIDATE_SITUATION_CHECK("*reject*");
        INVALIDATE_SITUATION_CHECK("reject*");
        INVALIDATE_SITUATION_CHECK("reject");
        INVALIDATE_SITUATION_CHECK("200g");
        INVALIDATE_SITUATION_CHECK("200G");
        INVALIDATE_SITUATION_CHECK("M");
        INVALIDATE_SITUATION_CHECK("K");
        INVALIDATE_SITUATION_CHECK("-1K");
        INVALIDATE_SITUATION_CHECK("1aK");
        INVALIDATE_SITUATION_CHECK("pegNo1");
        INVALIDATE_SITUATION_CHECK("-20");
        INVALIDATE_SITUATION_CHECK("12KM");
        INVALIDATE_SITUATION_CHECK("1K2M");
        INVALIDATE_SITUATION_CHECK("2000K0*reject*100");
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
};

TEST_F(token_bucket_throttling_controller_test, test_parse_env_basic_token_bucket_throttling)
{
    test_parse_env_basic_token_bucket_throttling();
}

TEST_F(token_bucket_throttling_controller_test, throttle_test) { throttle_test(); }
} // namespace utils
} // namespace dsn
