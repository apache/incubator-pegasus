/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <dsn/utility/string_conv.h>
#include <gtest/gtest.h>

TEST(string_conv, buf2int32)
{
    int32_t result;

    ASSERT_TRUE(dsn::buf2int32(std::to_string(0), result));
    ASSERT_EQ(result, 0);

    ASSERT_TRUE(dsn::buf2int32(std::to_string(42), result));
    ASSERT_EQ(result, 42);

    ASSERT_TRUE(dsn::buf2int32(std::to_string(-42), result));
    ASSERT_EQ(result, -42);

    ASSERT_TRUE(dsn::buf2int32(std::to_string(std::numeric_limits<int32_t>::min()), result));
    ASSERT_EQ(result, std::numeric_limits<int32_t>::min());

    ASSERT_TRUE(dsn::buf2int32(std::to_string(std::numeric_limits<int32_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<int32_t>::max());

    ASSERT_FALSE(dsn::buf2int32(std::to_string(std::numeric_limits<int64_t>::max()), result));
    ASSERT_FALSE(dsn::buf2int32(std::to_string(std::numeric_limits<int64_t>::min()), result));
    ASSERT_FALSE(dsn::buf2int32(std::to_string(std::numeric_limits<uint64_t>::max()), result));
}

TEST(string_conv, buf2int64)
{
    int64_t result;

    ASSERT_TRUE(dsn::buf2int64(std::to_string(0), result));
    ASSERT_EQ(result, 0);

    ASSERT_TRUE(dsn::buf2int64(std::to_string(42), result));
    ASSERT_EQ(result, 42);

    ASSERT_TRUE(dsn::buf2int64(std::to_string(-42), result));
    ASSERT_EQ(result, -42);

    ASSERT_TRUE(dsn::buf2int64(std::to_string(std::numeric_limits<int32_t>::min()), result));
    ASSERT_EQ(result, std::numeric_limits<int32_t>::min());

    ASSERT_TRUE(dsn::buf2int64(std::to_string(std::numeric_limits<int32_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<int32_t>::max());

    ASSERT_TRUE(dsn::buf2int64(std::to_string(std::numeric_limits<uint32_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<uint32_t>::max());

    ASSERT_TRUE(dsn::buf2int64(std::to_string(std::numeric_limits<int64_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<int64_t>::max());

    ASSERT_TRUE(dsn::buf2int64(std::to_string(std::numeric_limits<int64_t>::min()), result));
    ASSERT_EQ(result, std::numeric_limits<int64_t>::min());

    ASSERT_FALSE(dsn::buf2int64(std::to_string(std::numeric_limits<uint64_t>::max()), result));
}

TEST(string_conv, partial)
{
    int64_t result;
    ASSERT_FALSE(dsn::buf2int64("", result));
    ASSERT_FALSE(dsn::buf2int64(" ", result)) << result;
    ASSERT_FALSE(dsn::buf2int64("-", result)) << result;
    ASSERT_FALSE(dsn::buf2int64("123@@@", result));
    ASSERT_FALSE(dsn::buf2int64("@@@123", result));

    const int64_t int64_min = std::numeric_limits<int64_t>::min();
    const int64_t int64_max = std::numeric_limits<int64_t>::max();
    ASSERT_FALSE(dsn::buf2int64(std::to_string(int64_min) + std::to_string(int64_max), result));
    ASSERT_FALSE(dsn::buf2int64(std::to_string(int64_max) + std::to_string(int64_max), result));
}
