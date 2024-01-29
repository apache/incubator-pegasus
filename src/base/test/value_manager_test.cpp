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

#include <stdint.h>
#include <string>

#include "base/value_schema_manager.h"
#include "gtest/gtest.h"
#include "pegasus_value_schema.h"
#include "absl/strings/string_view.h"
#include "value_field.h"

using namespace pegasus;

extern std::string generate_value(value_schema *schema,
                                  uint32_t expire_ts,
                                  uint64_t time_tag,
                                  absl::string_view user_data);

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
