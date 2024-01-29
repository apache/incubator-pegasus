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

#include <stdint.h>
#include <limits>
#include <string>

#include "gtest/gtest.h"
#include "utils/endians.h"
#include "utils/enum_helper.h"

using namespace dsn;

TEST(endian, conversion)
{
    ASSERT_EQ(100, endian::ntoh(endian::hton(uint16_t(100))));
    ASSERT_EQ(100, endian::ntoh(endian::hton(uint32_t(100))));
    ASSERT_EQ(100, endian::ntoh(endian::hton(uint64_t(100))));
}

TEST(endian, write_and_read)
{
    {
        std::string data;
        data.resize(4);
        data_output(data).write_u32(100);
        ASSERT_EQ(100, data_input(data).read_u32());
    }

    {
        std::string data;
        data.resize(1);
        data_output(data).write_u8(100);
        ASSERT_EQ(100, data_input(data).read_u8());
    }

    {
        std::string data;
        data.resize(1000 * 8);

        data_output output(data);
        for (uint32_t value = 1; value < 1000000; value += 1000) {
            if (value < std::numeric_limits<uint16_t>::max()) {
                auto val_16 = static_cast<uint16_t>(value);
                output.write_u16(val_16);
            } else {
                output.write_u32(value);
            }
        }

        data_input input(data);
        for (uint32_t value = 1; value < 1000000; value += 1000) {
            if (value < std::numeric_limits<uint16_t>::max()) {
                ASSERT_EQ(value, input.read_u16());
            } else {
                ASSERT_EQ(value, input.read_u32());
            }
        }
    }
}
