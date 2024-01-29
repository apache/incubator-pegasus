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

#include "utils/string_conv.h"

#include "gtest/gtest.h"
#include "absl/strings/string_view.h"

TEST(string_conv, buf2bool)
{
    bool result = false;

    ASSERT_TRUE(dsn::buf2bool("true", result));
    ASSERT_EQ(result, true);

    ASSERT_TRUE(dsn::buf2bool("TrUe", result));
    ASSERT_EQ(result, true);

    ASSERT_FALSE(dsn::buf2bool("TrUe", result, false));

    ASSERT_TRUE(dsn::buf2bool("false", result));
    ASSERT_EQ(result, false);

    ASSERT_TRUE(dsn::buf2bool("FalSe", result));
    ASSERT_EQ(result, false);

    ASSERT_FALSE(dsn::buf2bool("TrUe", result, false));

    std::string str("true\0false", 10);
    ASSERT_FALSE(dsn::buf2bool(absl::string_view(str.data(), 3), result));
    ASSERT_TRUE(dsn::buf2bool(absl::string_view(str.data(), 4), result));
    ASSERT_EQ(result, true);
    ASSERT_FALSE(dsn::buf2bool(absl::string_view(str.data(), 5), result));
    ASSERT_FALSE(dsn::buf2bool(absl::string_view(str.data(), 6), result));
    ASSERT_FALSE(dsn::buf2bool(absl::string_view(str.data() + 5, 4), result));
    ASSERT_TRUE(dsn::buf2bool(absl::string_view(str.data() + 5, 5), result));
    ASSERT_EQ(result, false);
}

TEST(string_conv, buf2int32)
{
    int32_t result = -1;

    ASSERT_TRUE(dsn::buf2int32(std::to_string(0), result));
    ASSERT_EQ(result, 0);

    ASSERT_TRUE(dsn::buf2int32("0xbeef", result));
    ASSERT_EQ(result, 0xbeef);

    ASSERT_TRUE(dsn::buf2int32("0xBEEF", result));
    ASSERT_EQ(result, 0xbeef);

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

    // "\045" is "%", so the string length=5, otherwise(2th argument > 5) it will be reported
    // "global-buffer-overflow" error under AddressSanitizer check
    std::string str("123\0456", 5);
    ASSERT_TRUE(dsn::buf2int32(absl::string_view(str.data(), 2), result));
    ASSERT_EQ(result, 12);
    ASSERT_TRUE(dsn::buf2int32(absl::string_view(str.data(), 3), result));
    ASSERT_EQ(result, 123);
    ASSERT_FALSE(dsn::buf2int32(absl::string_view(str.data(), 4), result));
    ASSERT_FALSE(dsn::buf2int32(absl::string_view(str.data(), 5), result));
}

TEST(string_conv, buf2int64)
{
    int64_t result = -1;

    ASSERT_TRUE(dsn::buf2int64(std::to_string(0), result));
    ASSERT_EQ(result, 0);

    ASSERT_TRUE(dsn::buf2int64("0xdeadbeef", result));
    ASSERT_EQ(result, 0xdeadbeef);

    ASSERT_TRUE(dsn::buf2int64("0xDEADBEEF", result));
    ASSERT_EQ(result, 0xdeadbeef);

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

    // "\045" is "%", so the string length=5, otherwise(2th argument > 5) it will be reported
    // "global-buffer-overflow" error under AddressSanitizer check
    std::string str("123\0456", 5);
    ASSERT_TRUE(dsn::buf2int64(absl::string_view(str.data(), 2), result));
    ASSERT_EQ(result, 12);
    ASSERT_TRUE(dsn::buf2int64(absl::string_view(str.data(), 3), result));
    ASSERT_EQ(result, 123);
    ASSERT_FALSE(dsn::buf2int64(absl::string_view(str.data(), 4), result));
    ASSERT_FALSE(dsn::buf2int64(absl::string_view(str.data(), 5), result));
}

TEST(string_conv, buf2uint64)
{
    uint64_t result = 1;

    ASSERT_TRUE(dsn::buf2uint64(std::to_string(0), result));
    ASSERT_EQ(result, 0);

    ASSERT_TRUE(dsn::buf2uint64("-0", result));
    ASSERT_EQ(result, 0);

    ASSERT_FALSE(dsn::buf2uint64("-1", result));

    ASSERT_TRUE(dsn::buf2uint64("0xdeadbeef", result));
    ASSERT_EQ(result, 0xdeadbeef);

    ASSERT_TRUE(dsn::buf2uint64("0xDEADBEEF", result));
    ASSERT_EQ(result, 0xdeadbeef);

    ASSERT_TRUE(dsn::buf2uint64(std::to_string(42), result));
    ASSERT_EQ(result, 42);

    ASSERT_TRUE(dsn::buf2uint64(std::to_string(std::numeric_limits<int32_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<int32_t>::max());

    ASSERT_TRUE(dsn::buf2uint64(std::to_string(std::numeric_limits<uint32_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<uint32_t>::max());

    ASSERT_TRUE(dsn::buf2uint64(std::to_string(std::numeric_limits<uint64_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<uint64_t>::max());

    ASSERT_TRUE(dsn::buf2uint64(std::to_string(std::numeric_limits<uint64_t>::min()), result));
    ASSERT_EQ(result, std::numeric_limits<uint64_t>::min());

    // "\045" is "%", so the string length=5, otherwise(2th argument > 5) it will be reported
    // "global-buffer-overflow" error under AddressSanitizer check
    std::string str("123\0456", 5);
    ASSERT_TRUE(dsn::buf2uint64(absl::string_view(str.data(), 2), result));
    ASSERT_EQ(result, 12);
    ASSERT_TRUE(dsn::buf2uint64(absl::string_view(str.data(), 3), result));
    ASSERT_EQ(result, 123);
    ASSERT_FALSE(dsn::buf2uint64(absl::string_view(str.data(), 4), result));
    ASSERT_FALSE(dsn::buf2uint64(absl::string_view(str.data(), 5), result));
}

TEST(string_conv, buf2uint32)
{
    uint32_t result = 1;

    ASSERT_TRUE(dsn::buf2uint32(std::to_string(0), result));
    ASSERT_EQ(result, 0);

    ASSERT_TRUE(dsn::buf2uint32("-0", result));
    ASSERT_EQ(result, 0);

    ASSERT_FALSE(dsn::buf2uint32("-1", result));

    ASSERT_TRUE(dsn::buf2uint32("0xdeadbeef", result));
    ASSERT_EQ(result, 0xdeadbeef);

    ASSERT_TRUE(dsn::buf2uint32("0xDEADBEEF", result));
    ASSERT_EQ(result, 0xdeadbeef);

    ASSERT_TRUE(dsn::buf2uint32(std::to_string(42), result));
    ASSERT_EQ(result, 42);

    ASSERT_TRUE(dsn::buf2uint32(std::to_string(std::numeric_limits<int16_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<int16_t>::max());

    ASSERT_TRUE(dsn::buf2uint32(std::to_string(std::numeric_limits<uint16_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<uint16_t>::max());

    ASSERT_TRUE(dsn::buf2uint32(std::to_string(std::numeric_limits<uint32_t>::max()), result));
    ASSERT_EQ(result, std::numeric_limits<uint32_t>::max());

    ASSERT_TRUE(dsn::buf2uint32(std::to_string(std::numeric_limits<uint32_t>::min()), result));
    ASSERT_EQ(result, std::numeric_limits<uint32_t>::min());

    ASSERT_FALSE(dsn::buf2uint32(std::to_string(std::numeric_limits<uint64_t>::max()), result));

    // "\045" is "%", so the string length=5, otherwise(2th argument > 5) it will be reported
    // "global-buffer-overflow" error under AddressSanitizer check
    std::string str("123\0456", 5);
    ASSERT_TRUE(dsn::buf2uint32(absl::string_view(str.data(), 2), result));
    ASSERT_EQ(result, 12);
    ASSERT_TRUE(dsn::buf2uint32(absl::string_view(str.data(), 3), result));
    ASSERT_EQ(result, 123);
    ASSERT_FALSE(dsn::buf2uint32(absl::string_view(str.data(), 4), result));
    ASSERT_FALSE(dsn::buf2uint32(absl::string_view(str.data(), 5), result));
}

TEST(string_conv, int64_partial)
{
    int64_t result = 0;
    ASSERT_FALSE(dsn::buf2int64("", result));
    ASSERT_FALSE(dsn::buf2int64(" ", result)) << result;
    ASSERT_FALSE(dsn::buf2int64("-", result)) << result;
    ASSERT_FALSE(dsn::buf2int64("123@@@", result));
    ASSERT_FALSE(dsn::buf2int64("@@@123", result));
    ASSERT_FALSE(dsn::buf2int64("0xdeadbeeg", result));

    const int64_t int64_min = std::numeric_limits<int64_t>::min();
    const int64_t int64_max = std::numeric_limits<int64_t>::max();
    ASSERT_FALSE(dsn::buf2int64(std::to_string(int64_min) + std::to_string(int64_max), result));
    ASSERT_FALSE(dsn::buf2int64(std::to_string(int64_max) + std::to_string(int64_max), result));
}

TEST(string_conv, uint64_partial)
{
    uint64_t result = 0;
    ASSERT_FALSE(dsn::buf2uint64("", result));
    ASSERT_FALSE(dsn::buf2uint64(" ", result)) << result;
    ASSERT_FALSE(dsn::buf2uint64("-", result)) << result;
    ASSERT_FALSE(dsn::buf2uint64("123@@@", result));
    ASSERT_FALSE(dsn::buf2uint64("@@@123", result));
    ASSERT_FALSE(dsn::buf2uint64("0xdeadbeeg", result));

    ASSERT_FALSE(dsn::buf2uint64(std::to_string(-1), result));
    ASSERT_FALSE(
        dsn::buf2uint64(std::to_string(std::numeric_limits<uint64_t>::max()).append("0"), result));
}

TEST(string_conv, buf2double)
{
    double result = 0;

    ASSERT_TRUE(dsn::buf2double("1.1", result));
    ASSERT_DOUBLE_EQ(result, 1.1);

    ASSERT_TRUE(dsn::buf2double("0.0", result));
    ASSERT_DOUBLE_EQ(result, 0.0);
    ASSERT_TRUE(dsn::buf2double("-0.0", result));
    ASSERT_DOUBLE_EQ(result, 0.0);

    ASSERT_TRUE(dsn::buf2double("-1.1", result));
    ASSERT_DOUBLE_EQ(result, -1.1);

    ASSERT_TRUE(dsn::buf2double("1.2e3", result));
    ASSERT_DOUBLE_EQ(result, 1200.0);

    ASSERT_TRUE(dsn::buf2double("1.2E3", result));
    ASSERT_DOUBLE_EQ(result, 1200.0);

    ASSERT_TRUE(dsn::buf2double("1e0", result));
    ASSERT_DOUBLE_EQ(result, 1.0);

    ASSERT_TRUE(dsn::buf2double("0x1.2p3", result));
    ASSERT_DOUBLE_EQ(result, 0x1.2p3);
    ASSERT_DOUBLE_EQ(result, 9.0);

    ASSERT_TRUE(dsn::buf2double("0X1.2P3", result));
    ASSERT_DOUBLE_EQ(result, 0x1.2p3);
    ASSERT_DOUBLE_EQ(result, 9.0);

    /// bad case
    ASSERT_FALSE(dsn::buf2double("nan", result));
    ASSERT_FALSE(dsn::buf2double("NaN", result));
    ASSERT_FALSE(dsn::buf2double("-nan", result));
    ASSERT_FALSE(dsn::buf2double("-NAN", result));
    ASSERT_FALSE(dsn::buf2double("inf", result));
    ASSERT_FALSE(dsn::buf2double("-INF", result));
    ASSERT_FALSE(dsn::buf2double("INFINITY", result));
    ASSERT_FALSE(dsn::buf2double("abc", result));
    ASSERT_FALSE(dsn::buf2double("1.18973e+4932", result));
}
