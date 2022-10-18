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

#include "utils/join_point.h"
#include <gtest/gtest.h>

namespace dsn {

class join_point_test : public ::testing::Test
{
public:
};

void advice1() {}

// smoke test
TEST_F(join_point_test, add_pure_functions)
{
    join_point<void> jp("test");
    jp.put_back(advice1, "test");
    jp.put_front(advice1, "test");
    jp.put_native(advice1);
    jp.execute();

    ASSERT_STREQ(jp.name(), "test");
}

TEST_F(join_point_test, adding_order)
{
    join_point<void, int> jp("test");

    std::vector<int> vec;
    jp.put_back([&](int val) { vec.push_back(1); }, "test");
    jp.put_back([&](int val) { vec.push_back(2); }, "test");
    jp.put_back([&](int val) { vec.push_back(3); }, "test");
    jp.put_front([&](int val) { vec.push_back(4); }, "test");
    jp.put_front([&](int val) { vec.push_back(5); }, "test");
    jp.execute(0);

    ASSERT_EQ(vec, std::vector<int>({5, 4, 1, 2, 3}));
}

TEST_F(join_point_test, with_return_value)
{
    join_point<double, int, double, std::string> jp("test");

    std::vector<int> vec;
    std::string expected_str;

    jp.put_back([&](int, double, std::string) { vec.push_back(1); }, "test");
    jp.put_back([&](int, double, std::string) { vec.push_back(2); }, "test");
    jp.put_native([&](int b, double c, std::string str) -> double {
        vec.push_back(3);
        expected_str = str;
        return c + b;
    });
    double exec_res = jp.execute(5, 0.5, std::string("abc"), 0.0);

    ASSERT_EQ(vec, std::vector<int>({3, 1, 2}));
    ASSERT_EQ(exec_res, 5.5);
    ASSERT_EQ(expected_str, "abc");
}

} // namespace dsn
