// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/endians.h>

#include <gtest/gtest.h>

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
