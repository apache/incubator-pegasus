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

# include <dsn/internal/env_provider.h>
# include <gtest/gtest.h>
# include "env.sim.h"

using namespace ::dsn;
//
//void test_env(env_provider& ev)
//{
//    uint64_t xs[] = {
//        0,
//        (uint64_t)-1LL,
//        0xdeadbeef
//        };
//
//    for (auto& x : xs)
//    {
//        auto r = ev.random64(x, x);
//        EXPECT_EQ(r, x);
//
//        r = ev.random64(x, x + 1);
//        EXPECT_TRUE(r == x || r == (x + 1));
//    }
//}
//
//TEST(tools, env_native)
//{
//    env_provider ev(nullptr);
//    test_env(ev);
//}

