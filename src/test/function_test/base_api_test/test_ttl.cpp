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

std::string ttl_hash_key = "ttl_test_hash_key";
std::string ttl_test_sort_key_0 = "ttl_test_sort_key_0";
std::string ttl_test_sort_key_1 = "ttl_test_sort_key_1";
std::string ttl_test_sort_key_2 = "ttl_test_sort_key_2";
std::string ttl_test_value_0 = "ttl_test_value_0";
std::string ttl_test_value_1 = "ttl_test_value_1";
std::string ttl_test_value_2 = "ttl_test_value_2";
int default_ttl = 3600;
int specify_ttl = 5;
int sleep_for_expiring = 10;
int sleep_for_envs_effect = 31;
int error_allow = 2;
int timeout = 5000;

class ttl : public test_util
{
public:
    void SetUp() override
    {
        test_util::SetUp();
        set_default_ttl(0);
    }

    void TearDown() override { ASSERT_EQ(dsn::ERR_OK, ddl_client->drop_app(app_name_, 0)); }

    void set_default_ttl(int ttl)
    {
        std::map<std::string, std::string> envs;
        ddl_client->get_app_envs(client->get_app_name(), envs);

        std::string env = envs[TABLE_LEVEL_DEFAULT_TTL];
        if ((env.empty() && ttl != 0) || env != std::to_string(ttl)) {
            auto response = ddl_client->set_app_envs(
                client->get_app_name(), {TABLE_LEVEL_DEFAULT_TTL}, {std::to_string(ttl)});
            ASSERT_EQ(true, response.is_ok());
            ASSERT_EQ(ERR_OK, response.get_value().err);

            // wait envs to be synced.
            std::this_thread::sleep_for(std::chrono::seconds(sleep_for_envs_effect));
        }
    }
};

TEST_F(ttl, set_without_default_ttl)
{
    // set with ttl
    int ret =
        client->set(ttl_hash_key, ttl_test_sort_key_1, ttl_test_value_1, timeout, specify_ttl);
    ASSERT_EQ(PERR_OK, ret);

    std::string value;
    ret = client->get(ttl_hash_key, ttl_test_sort_key_1, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_1, value);

    int ttl_seconds;
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_TRUE(ttl_seconds > specify_ttl - error_allow && ttl_seconds <= specify_ttl)
        << "ttl is " << ttl_seconds;

    // set without ttl
    ret = client->set(ttl_hash_key, ttl_test_sort_key_2, ttl_test_value_2);
    ASSERT_EQ(PERR_OK, ret);

    ret = client->get(ttl_hash_key, ttl_test_sort_key_2, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_2, value);

    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_seconds, -1) << "ttl is " << ttl_seconds;

    // sleep a while
    std::this_thread::sleep_for(std::chrono::seconds(sleep_for_expiring));

    // check expired one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    ret = client->get(ttl_hash_key, ttl_test_sort_key_1, value);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // check exist one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_seconds, -1) << "ttl is " << ttl_seconds;

    ret = client->get(ttl_hash_key, ttl_test_sort_key_2, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_2, value);

    // trigger a manual compaction
    auto response = ddl_client->set_app_envs(client->get_app_name(),
                                             {MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY},
                                             {std::to_string(time(nullptr))});
    ASSERT_EQ(true, response.is_ok());
    ASSERT_EQ(ERR_OK, response.get_value().err);

    // wait envs to be synced, and manual lcompaction has been finished.
    std::this_thread::sleep_for(std::chrono::seconds(sleep_for_envs_effect));

    // check expired one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    ret = client->get(ttl_hash_key, ttl_test_sort_key_1, value);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // check exist one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_seconds, -1) << "ttl is " << ttl_seconds;

    ret = client->get(ttl_hash_key, ttl_test_sort_key_2, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_2, value);
}

TEST_F(ttl, set_with_default_ttl)
{
    // set without ttl
    int ret = client->set(ttl_hash_key, ttl_test_sort_key_0, ttl_test_value_0);
    ASSERT_EQ(PERR_OK, ret);

    // set default_ttl
    set_default_ttl(default_ttl);

    // set with ttl
    ret = client->set(ttl_hash_key, ttl_test_sort_key_1, ttl_test_value_1, timeout, specify_ttl);
    ASSERT_EQ(PERR_OK, ret);

    std::string value;
    ret = client->get(ttl_hash_key, ttl_test_sort_key_1, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_1, value);

    int ttl_seconds;
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_TRUE(ttl_seconds >= specify_ttl - error_allow && ttl_seconds <= specify_ttl)
        << "ttl is " << ttl_seconds;

    // set without ttl
    ret = client->set(ttl_hash_key, ttl_test_sort_key_2, ttl_test_value_2);
    ASSERT_EQ(PERR_OK, ret);

    ret = client->get(ttl_hash_key, ttl_test_sort_key_2, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_2, value);

    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_TRUE(ttl_seconds >= default_ttl - error_allow && ttl_seconds <= default_ttl)
        << "ttl is " << ttl_seconds;

    // sleep a while
    std::this_thread::sleep_for(std::chrono::seconds(sleep_for_expiring));

    // check forever one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_0, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_seconds, -1) << "ttl is " << ttl_seconds;

    // check expired one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    ret = client->get(ttl_hash_key, ttl_test_sort_key_1, value);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // check exist one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_TRUE(ttl_seconds >= default_ttl - sleep_for_expiring - error_allow &&
                ttl_seconds <= default_ttl - sleep_for_expiring + error_allow)
        << "ttl is " << ttl_seconds;

    ret = client->get(ttl_hash_key, ttl_test_sort_key_2, value);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_EQ(ttl_test_value_2, value);

    // trigger a manual compaction
    auto response = ddl_client->set_app_envs(client->get_app_name(),
                                             {MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY},
                                             {std::to_string(time(nullptr))});
    ASSERT_EQ(true, response.is_ok());
    ASSERT_EQ(ERR_OK, response.get_value().err);

    // wait envs to be synced, and manual compaction has been finished.
    std::this_thread::sleep_for(std::chrono::seconds(sleep_for_envs_effect));

    // check forever one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_0, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_TRUE(ttl_seconds >= default_ttl - sleep_for_envs_effect - error_allow &&
                ttl_seconds <= default_ttl + error_allow)
        << "ttl is " << ttl_seconds;

    // check expired one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_1, ttl_seconds);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    ret = client->get(ttl_hash_key, ttl_test_sort_key_1, value);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    // check exist one
    ret = client->ttl(ttl_hash_key, ttl_test_sort_key_2, ttl_seconds);
    ASSERT_EQ(PERR_OK, ret);
    ASSERT_TRUE(
        ttl_seconds >= default_ttl - sleep_for_expiring - sleep_for_envs_effect - error_allow &&
        ttl_seconds <= default_ttl - sleep_for_expiring - sleep_for_envs_effect + error_allow)
        << "ttl is " << ttl_seconds;
}
