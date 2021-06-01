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

#include "value_schema_manager.h"
#include <gtest/gtest.h>

using namespace pegasus;

extern std::string generate_value(value_schema *schema,
                                  uint32_t expire_ts,
                                  uint64_t time_tag,
                                  dsn::string_view user_data);

TEST(value_schema_manager, get_latest_value_schema)
{
    auto schema = value_schema_manager::instance().get_latest_value_schema();
    ASSERT_EQ(data_version::VERSION_MAX, schema->version());
}

TEST(value_schema_manager, get_value_schema)
{
    struct test_case
    {
        uint32_t version;
        bool schema_exist;
    } tests[] = {
        {pegasus::data_version::VERSION_0, true},
        {pegasus::data_version::VERSION_1, true},
        {pegasus::data_version::VERSION_2, true},
        {pegasus::data_version::VERSION_MAX + 1, false},
    };

    for (const auto &t : tests) {
        auto schema = value_schema_manager::instance().get_value_schema(t.version);
        if (t.schema_exist) {
            ASSERT_NE(schema, nullptr);
            ASSERT_EQ(t.version, schema->version());
        } else {
            ASSERT_EQ(schema, nullptr);
        }
    }
}

TEST(pegasus_value_manager, get_value_schema)
{
    struct test_case
    {
        uint32_t meta_store_data_version;
        uint32_t value_schema_version;
        data_version expect_version;
    } tests[] = {
        {0, 0, pegasus::data_version::VERSION_0},
        {1, 0, pegasus::data_version::VERSION_1},
        {0, 1, pegasus::data_version::VERSION_0},
        {1, 1, pegasus::data_version::VERSION_1},
        {0, 2, pegasus::data_version::VERSION_2},
        {1, 2, pegasus::data_version::VERSION_2},
    };

    for (const auto &t : tests) {
        auto generate_schema =
            value_schema_manager::instance().get_value_schema(t.value_schema_version);
        std::string raw_value = generate_value(generate_schema, 0, 0, "");

        auto schema =
            value_schema_manager::instance().get_value_schema(t.meta_store_data_version, raw_value);
        ASSERT_EQ(t.expect_version, schema->version());
    }
}

TEST(pegasus_value_manager, check_if_ts_expired)
{
    struct test_case
    {
        uint32_t epoch_now;
        uint32_t expire_ts;
        bool res_value;
    } tests[] = {
        {0, UINT32_MAX, false},
        {100, 100, true},
        {100, 99, true},
        {100, 101, false},
    };

    for (const auto &t : tests) {
        ASSERT_EQ(t.res_value, check_if_ts_expired(t.epoch_now, t.expire_ts));
    }
}

TEST(pegasus_value_manager, extract_timestamp_from_timetag)
{
    uint64_t deadbeaf = 0xdeadbeaf;
    uint64_t timetag = deadbeaf << 8u | 0xab;
    auto res = extract_timestamp_from_timetag(timetag);
    ASSERT_EQ(res, deadbeaf);
}

TEST(pegasus_value_manager, generate_timetag)
{
    uint64_t timestamp = 10086;
    uint8_t cluster_id = 1;
    bool deleted_tag = false;
    auto res = generate_timetag(timestamp, cluster_id, deleted_tag);

    ASSERT_EQ(res >> 8u, timestamp);
    ASSERT_EQ((res & 0xFF) >> 1u, cluster_id);
    ASSERT_EQ(res & 0x01, deleted_tag);
}

TEST(pegasus_value_manager, pegasus_extract_expire_ts)
{
    struct test_case
    {
        uint32_t version;
        uint32_t expire_ts;
    } tests[] = {
        {0, 10086},
        {1, 10086},
        {2, 10086},
    };

    for (const auto &test : tests) {
        // generate data for test
        auto schema = value_schema_manager::instance().get_value_schema(test.version);
        auto value = generate_value(schema, test.expire_ts, 0, "user_data");

        auto expect_expire_ts = pegasus_extract_expire_ts(test.version, value);
        ASSERT_EQ(expect_expire_ts, test.expire_ts);
    }
}

TEST(pegasus_value_manager, pegasus_extract_timetag)
{
    struct test_case
    {
        uint32_t version;
        uint32_t time_tag;
    } tests[] = {
        {2, 10086},
    };

    for (const auto &test : tests) {
        // generate data for test
        auto schema = value_schema_manager::instance().get_value_schema(test.version);
        auto value = generate_value(schema, 0, test.time_tag, "user_data");

        auto time_tag = pegasus_extract_timetag(test.version, value);
        ASSERT_EQ(time_tag, test.time_tag);
    }
}

TEST(pegasus_value_manager, pegasus_extract_user_data)
{
    struct test_case
    {
        uint32_t version;
        std::string user_data;
    } tests[] = {
        {0, "user_data"},
        {1, "user_data"},
        {2, "user_data"},
    };

    for (const auto &test : tests) {
        // generate data for test
        auto schema = value_schema_manager::instance().get_value_schema(test.version);
        auto value = generate_value(schema, 0, 0, test.user_data);

        dsn::blob user_data;
        pegasus_extract_user_data(test.version, std::move(value), user_data);
        ASSERT_EQ(user_data, test.user_data);
    }
}

TEST(pegasus_value_manager, pegasus_update_expire_ts)
{
    struct test_case
    {
        uint32_t version;
        uint32_t expire_ts;
        uint32_t update_expire_ts;
    } tests[] = {
        {0, 0, 10086},
        {1, 0, 10086},
        {2, 0, 10086},
    };

    for (const auto &test : tests) {
        // generate data for test
        auto schema = value_schema_manager::instance().get_value_schema(test.version);
        auto value = generate_value(schema, test.expire_ts, 0, "user_data");

        // update expire timestamp
        pegasus_update_expire_ts(test.version, value, test.update_expire_ts);

        auto new_expire_ts = pegasus_extract_expire_ts(test.version, value);
        ASSERT_EQ(new_expire_ts, test.update_expire_ts);
    }
}
