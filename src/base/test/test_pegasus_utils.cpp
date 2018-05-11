// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <string>

#include <gtest/gtest.h>

#include "pegasus_utils.h"

using namespace pegasus::utils;

TEST(base, buf2int_string_view)
{
    int result;
    ASSERT_TRUE(buf2int("1", result));
    ASSERT_EQ(1, result);

    ASSERT_TRUE(buf2int("0", result));
    ASSERT_EQ(0, result);

    ASSERT_TRUE(buf2int("-1", result));
    ASSERT_EQ(-1, result);

    ASSERT_TRUE(buf2int(std::to_string(INT_MIN), result));
    ASSERT_EQ(INT_MIN, result);

    ASSERT_TRUE(buf2int(std::to_string(INT_MAX), result));
    ASSERT_EQ(INT_MAX, result);

    ASSERT_FALSE(buf2int(std::to_string(INT_MIN)+"0", result));
    ASSERT_FALSE(buf2int(std::to_string(INT_MIN)+"0", result));
    ASSERT_FALSE(buf2int(std::to_string(LLONG_MAX), result));
    ASSERT_FALSE(buf2int("", result));
    ASSERT_FALSE(buf2int("a", result));
    ASSERT_FALSE(buf2int("9a", result));
    ASSERT_FALSE(buf2int("?", result));
}

TEST(base, buf2uint64_string_view)
{
    uint64_t result;
    ASSERT_TRUE(buf2uint64("1", result));
    ASSERT_EQ(1, result);

    ASSERT_TRUE(buf2uint64("0", result));
    ASSERT_EQ(0, result);

    ASSERT_TRUE(buf2uint64("-1", result));
    ASSERT_EQ(-1, result);

    ASSERT_TRUE(buf2uint64(std::to_string(ULLONG_MAX), result));
    ASSERT_EQ(ULLONG_MAX, result);

    ASSERT_FALSE(buf2uint64(std::to_string(ULLONG_MAX)+"0", result));
    ASSERT_FALSE(buf2uint64("", result));
    ASSERT_FALSE(buf2uint64("a", result));
    ASSERT_FALSE(buf2uint64("9a", result));
    ASSERT_FALSE(buf2uint64("-1", result));
    ASSERT_FALSE(buf2uint64("?", result));
}
