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

#include "base/pegasus_value_schema.h"

#include <limits>

#include "gtest/gtest.h"

using namespace pegasus;

TEST(value_schema, generate_and_extract_v1_v0)
{
    struct test_case
    {
        int value_schema_version;

        uint32_t expire_ts;
        uint64_t timetag;
        std::string user_data;
    } tests[] = {
        {1, 1000, 10001, ""},
        {1, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), "pegasus"},
        {1, std::numeric_limits<uint32_t>::max(), std::numeric_limits<uint64_t>::max(), ""},

        {0, 1000, 0, ""},
        {0, std::numeric_limits<uint32_t>::max(), 0, "pegasus"},
        {0, std::numeric_limits<uint32_t>::max(), 0, ""},
        {0, 0, 0, "a"},
    };

    for (auto &t : tests) {
        pegasus_value_generator gen;
        rocksdb::SliceParts sparts =
            gen.generate_value(t.value_schema_version, t.user_data, t.expire_ts, t.timetag);

        std::string raw_value;
        for (int i = 0; i < sparts.num_parts; i++) {
            raw_value += sparts.parts[i].ToString();
        }

        ASSERT_EQ(t.expire_ts, pegasus_extract_expire_ts(t.value_schema_version, raw_value));

        if (t.value_schema_version == 1) {
            ASSERT_EQ(t.timetag, pegasus_extract_timetag(t.value_schema_version, raw_value));
        }

        dsn::blob user_data;
        pegasus_extract_user_data(t.value_schema_version, std::move(raw_value), user_data);
        ASSERT_EQ(t.user_data, user_data.to_string());
    }
}
