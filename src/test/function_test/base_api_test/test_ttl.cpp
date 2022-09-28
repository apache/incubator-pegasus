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

#include <thread>

#include <dsn/dist/replication/replication_ddl_client.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>

#include "base/pegasus_const.h"
#include "test/function_test/utils/test_util.h"

using namespace ::dsn;
using namespace ::pegasus;

class ttl_test : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();
        ASSERT_NO_FATAL_FAILURE(set_default_ttl_secs(0));
    }

    void TearDown() override { ASSERT_EQ(dsn::ERR_OK, ddl_client_->drop_app(app_name_, 0)); }

    void set_default_ttl_secs(int32_t ttl)
    {
        std::map<std::string, std::string> envs;
        ASSERT_EQ(ERR_OK, ddl_client_->get_app_envs(client_->get_app_name(), envs));

        std::string env = envs[TABLE_LEVEL_DEFAULT_TTL];
        if ((env.empty() && ttl != 0) || env != std::to_string(ttl)) {
            auto response = ddl_client_->set_app_envs(
                client_->get_app_name(), {TABLE_LEVEL_DEFAULT_TTL}, {std::to_string(ttl)});
            ASSERT_EQ(true, response.is_ok());
            ASSERT_EQ(ERR_OK, response.get_value().err);

            // wait envs to be synced.
            std::this_thread::sleep_for(std::chrono::seconds(sleep_secs_for_envs_effect));
        }
    }

protected:
    const std::string ttl_hash_key = "ttl_test_hash_key";
    const std::string ttl_test_sort_key_0 = "ttl_test_sort_key_0";
    const std::string ttl_test_sort_key_1 = "ttl_test_sort_key_1";
    const std::string ttl_test_sort_key_2 = "ttl_test_sort_key_2";
    const std::string ttl_test_value_0 = "ttl_test_value_0";
    const std::string ttl_test_value_1 = "ttl_test_value_1";
    const std::string ttl_test_value_2 = "ttl_test_value_2";

    const int32_t default_ttl_secs = 3600;
    const int32_t specify_ttl_secs = 5;
    const int32_t sleep_for_expiring = 10;
    const int32_t sleep_secs_for_envs_effect = 31;
    const int32_t error_allow = 2;
    const int32_t timeout_ms = 5000;
};

TEST_F(ttl_test, set_without_default_ttl)
{
    // set with ttl
    ASSERT_EQ(
        PERR_OK,
        client_->set(
            ttl_hash_key, ttl_test_sort_key_1, ttl_test_value_1, timeout_ms, specify_ttl_secs));

    std::string value;
    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_1, value));
    ASSERT_EQ(ttl_test_value_1, value);

    int32_t ttl_seconds;
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds));
    ASSERT_GT(ttl_seconds, specify_ttl_secs - error_allow);
    ASSERT_LE(ttl_seconds, specify_ttl_secs);

    // set without ttl
    ASSERT_EQ(PERR_OK, client_->set(ttl_hash_key, ttl_test_sort_key_2, ttl_test_value_2));

    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_2, value));
    ASSERT_EQ(ttl_test_value_2, value);

    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds));
    ASSERT_EQ(ttl_seconds, -1);

    // sleep a while
    std::this_thread::sleep_for(std::chrono::seconds(sleep_for_expiring));

    // check expired one
    ASSERT_EQ(PERR_NOT_FOUND, client_->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds));
    ASSERT_EQ(PERR_NOT_FOUND, client_->get(ttl_hash_key, ttl_test_sort_key_1, value));

    // check exist one
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds));
    ASSERT_EQ(ttl_seconds, -1);

    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_2, value));
    ASSERT_EQ(ttl_test_value_2, value);

    // trigger a manual compaction
    auto response = ddl_client_->set_app_envs(client_->get_app_name(),
                                              {MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY},
                                              {std::to_string(time(nullptr))});
    ASSERT_EQ(true, response.is_ok());
    ASSERT_EQ(ERR_OK, response.get_value().err);

    // wait envs to be synced, and manual lcompaction has been finished.
    std::this_thread::sleep_for(std::chrono::seconds(sleep_secs_for_envs_effect));

    // check expired one
    ASSERT_EQ(PERR_NOT_FOUND, client_->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds));
    ASSERT_EQ(PERR_NOT_FOUND, client_->get(ttl_hash_key, ttl_test_sort_key_1, value));

    // check exist one
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds));
    ASSERT_EQ(ttl_seconds, -1);

    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_2, value));
    ASSERT_EQ(ttl_test_value_2, value);
}

TEST_F(ttl_test, set_with_default_ttl)
{
    // set without ttl
    ASSERT_EQ(PERR_OK, client_->set(ttl_hash_key, ttl_test_sort_key_0, ttl_test_value_0));

    // set default_ttl_secs
    ASSERT_NO_FATAL_FAILURE(set_default_ttl_secs(default_ttl_secs));

    // set with ttl
    ASSERT_EQ(
        PERR_OK,
        client_->set(
            ttl_hash_key, ttl_test_sort_key_1, ttl_test_value_1, timeout_ms, specify_ttl_secs));

    std::string value;
    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_1, value));
    ASSERT_EQ(ttl_test_value_1, value);

    int32_t ttl_seconds;
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds));

    ASSERT_GE(ttl_seconds, specify_ttl_secs - error_allow);
    ASSERT_LE(ttl_seconds, specify_ttl_secs);

    // set without ttl
    ASSERT_EQ(PERR_OK, client_->set(ttl_hash_key, ttl_test_sort_key_2, ttl_test_value_2));

    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_2, value));
    ASSERT_EQ(ttl_test_value_2, value);

    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds));
    ASSERT_GE(ttl_seconds, default_ttl_secs - error_allow);
    ASSERT_LE(ttl_seconds, default_ttl_secs);

    // sleep a while
    std::this_thread::sleep_for(std::chrono::seconds(sleep_for_expiring));

    // check forever one
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_0, ttl_seconds));
    ASSERT_EQ(ttl_seconds, -1);

    // check expired one
    ASSERT_EQ(PERR_NOT_FOUND, client_->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds));
    ASSERT_EQ(PERR_NOT_FOUND, client_->get(ttl_hash_key, ttl_test_sort_key_1, value));

    // check exist one
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds));
    ASSERT_NEAR(ttl_seconds, default_ttl_secs - sleep_for_expiring, error_allow);

    ASSERT_EQ(PERR_OK, client_->get(ttl_hash_key, ttl_test_sort_key_2, value));
    ASSERT_EQ(ttl_test_value_2, value);

    // trigger a manual compaction
    auto response = ddl_client_->set_app_envs(client_->get_app_name(),
                                              {MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY},
                                              {std::to_string(time(nullptr))});
    ASSERT_EQ(true, response.is_ok());
    ASSERT_EQ(ERR_OK, response.get_value().err);

    // wait envs to be synced, and manual compaction has been finished.
    std::this_thread::sleep_for(std::chrono::seconds(sleep_secs_for_envs_effect));

    // check forever one
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_0, ttl_seconds));
    ASSERT_GE(ttl_seconds, default_ttl_secs - sleep_secs_for_envs_effect - error_allow);
    ASSERT_LE(ttl_seconds, default_ttl_secs + error_allow);

    // check expired one
    ASSERT_EQ(PERR_NOT_FOUND, client_->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds));
    ASSERT_EQ(PERR_NOT_FOUND, client_->get(ttl_hash_key, ttl_test_sort_key_1, value));

    // check exist one
    ASSERT_EQ(PERR_OK, client_->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds));
    ASSERT_NEAR(ttl_seconds,
                default_ttl_secs - sleep_for_expiring - sleep_secs_for_envs_effect,
                error_allow);
}
