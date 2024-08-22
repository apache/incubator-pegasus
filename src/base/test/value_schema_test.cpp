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

#include <rocksdb/slice.h>
#include <stdint.h>
#include <array>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/pegasus_value_schema.h"
#include "base/value_schema_manager.h"
#include "gtest/gtest.h"
#include "utils/blob.h"
#include <string_view>
#include "value_field.h"

using namespace pegasus;

uint32_t extract_expire_ts(value_schema *schema, const std::string &raw_value)
{
    auto field = schema->extract_field(raw_value, pegasus::value_field_type::EXPIRE_TIMESTAMP);
    auto expire_ts_field = static_cast<expire_timestamp_field *>(field.get());
    return expire_ts_field->expire_ts;
}

uint64_t extract_time_tag(value_schema *schema, const std::string &raw_value)
{
    auto field = schema->extract_field(raw_value, pegasus::value_field_type::TIME_TAG);
    auto time_field = static_cast<time_tag_field *>(field.get());
    return time_field->time_tag;
}

std::string generate_value(value_schema *schema,
                           uint32_t expire_ts,
                           uint64_t time_tag,
                           std::string_view user_data)
{
    std::string write_buf;
    std::vector<rocksdb::Slice> write_slices;
    value_params params{write_buf, write_slices};
    params.fields[value_field_type::EXPIRE_TIMESTAMP] =
        std::make_unique<expire_timestamp_field>(expire_ts);
    params.fields[value_field_type::TIME_TAG] = std::make_unique<time_tag_field>(time_tag);
    params.fields[value_field_type::USER_DATA] = std::make_unique<user_data_field>(user_data);

    rocksdb::SliceParts sparts = schema->generate_value(params);
    std::string raw_value;
    for (int i = 0; i < sparts.num_parts; i++) {
        raw_value += sparts.parts[i].ToString();
    }
    return raw_value;
}

TEST(value_schema, generate_and_extract)
{
    struct test_case
    {
        uint32_t data_version;
        uint32_t expire_ts;
        uint64_t time_tag;
        std::string user_data;
    } tests[] = {
        {0, 1000, 0, ""},
        {0, std::numeric_limits<uint32_t>::max(), 0, "pegasus"},
        {0, std::numeric_limits<uint32_t>::max(), 0, ""},
        {0, 0, 0, "a"},

        {1, 1000, 10001, ""},
        {1, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), "pegasus"},
        {1, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), ""},
        {1, 0, 0, "a"},

        {2, 1000, 10001, ""},
        {2, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), "pegasus"},
        {2, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), ""},
        {2, 0, 0, "a"},
    };

    for (const auto &t : tests) {
        auto schema = value_schema_manager::instance().get_value_schema(t.data_version);
        std::string raw_value = generate_value(schema, t.expire_ts, t.time_tag, t.user_data);

        ASSERT_EQ(t.expire_ts, extract_expire_ts(schema, raw_value));
        if (t.data_version >= 1) {
            ASSERT_EQ(t.time_tag, extract_time_tag(schema, raw_value));
        }

        dsn::blob user_data = schema->extract_user_data(std::move(raw_value));
        ASSERT_EQ(t.user_data, user_data.to_string());
    }
}

TEST(value_schema, update_expire_ts)
{
    struct test_case
    {
        uint32_t data_version;
        uint32_t expire_ts;
        uint32_t update_expire_ts;
    } tests[] = {
        {0, 1000, 10086},
        {1, 1000, 10086},
        {2, 1000, 10086},
    };

    for (const auto &t : tests) {
        std::string write_buf;
        std::vector<rocksdb::Slice> write_slices;
        auto schema = value_schema_manager::instance().get_value_schema(t.data_version);
        std::string raw_value = generate_value(schema, t.expire_ts, 0, "");

        std::unique_ptr<value_field> field =
            std::make_unique<expire_timestamp_field>(t.update_expire_ts);
        schema->update_field(raw_value, std::move(field));
        ASSERT_EQ(t.update_expire_ts, extract_expire_ts(schema, raw_value));
    }
}
