// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <thread>

#include <dsn/dist/replication/replication_ddl_client.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>

#include "base/pegasus_const.h"

using namespace ::dsn;
using namespace ::pegasus;

extern pegasus_client *client;
extern std::shared_ptr<replication::replication_ddl_client> ddl_client;

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
int sleep_for_envs_effect = 65;
int error_allow = 2;
int timeout = 5000;

void set_default_ttl(int ttl)
{
    std::map<std::string, std::string> envs;
    ddl_client->get_app_envs(client->get_app_name(), envs);

    std::string env = envs[TABLE_LEVEL_DEFAULT_TTL];
    if ((env.empty() && ttl != 0) || env != std::to_string(ttl)) {
        dsn::error_code ec = ddl_client->set_app_envs(
            client->get_app_name(), {TABLE_LEVEL_DEFAULT_TTL}, {std::to_string(ttl)});
        ASSERT_EQ(ERR_OK, ec);

        // wait envs to be synced.
        std::this_thread::sleep_for(std::chrono::seconds(sleep_for_envs_effect));
    }
}

TEST(ttl, set_without_default_ttl)
{
    // unset default_ttl
    set_default_ttl(0);

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
    dsn::error_code ec = ddl_client->set_app_envs(client->get_app_name(),
                                                  {MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY},
                                                  {std::to_string(time(nullptr))});
    ASSERT_EQ(ERR_OK, ec);

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

TEST(ttl, set_with_default_ttl)
{
    // unset default_ttl
    set_default_ttl(0);

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
    dsn::error_code ec = ddl_client->set_app_envs(client->get_app_name(),
                                                  {MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY},
                                                  {std::to_string(time(nullptr))});
    ASSERT_EQ(ERR_OK, ec);

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
