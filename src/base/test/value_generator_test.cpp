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

#include "value_generator.h"
#include "value_schema_manager.h"

#include <gtest/gtest.h>

using namespace pegasus;

TEST(value_generator, generate_value)
{
    value_generator gen;
    std::string user_data = "user_data";
    uint32_t expire_ts = 10086;
    uint64_t timetag = 1000;

    auto sparts = gen.generate_value(user_data, expire_ts, timetag);
    std::string value;
    for (int i = 0; i < sparts.num_parts; i++) {
        value += sparts.parts[i].ToString();
    }

    ASSERT_EQ(pegasus_extract_expire_ts(VERSION_MAX, value), expire_ts);
    ASSERT_EQ(pegasus_extract_timetag(VERSION_MAX, value), timetag);
    dsn::blob extract_user_data;
    pegasus_extract_user_data(VERSION_MAX, std::move(value), extract_user_data);
    ASSERT_EQ(extract_user_data.to_string(), user_data);
}
