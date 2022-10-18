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

/*
 * Description:
 *     Unit-test for semaphore.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "utils/hpc_locks/sema.h"
#include <gtest/gtest.h>
#include <thread>

TEST(core, Semaphore)
{
    Semaphore s;

    ASSERT_FALSE(s.wait(10));

    s.signal();
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.wait(10));

    s.signal(2);
    ASSERT_TRUE(s.wait(10));
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.wait(10));

    std::thread t1([&s]() {
        ASSERT_FALSE(s.wait(10));

        s.wait();
    });

    std::thread t2([&s]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        s.signal(2);

        s.wait();
    });

    t1.join();
    t2.join();
}

TEST(core, LightweightSemaphore)
{
    LightweightSemaphore s;

    ASSERT_FALSE(s.wait(10));

    s.signal();
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.tryWait());
    ASSERT_FALSE(s.wait(10));

    s.signal(2);
    ASSERT_TRUE(s.tryWait());
    ASSERT_TRUE(s.wait(10));
    ASSERT_FALSE(s.tryWait());
    ASSERT_FALSE(s.wait(10));

    bool flag = false;

    std::thread t1([&s, &flag]() {
        ASSERT_FALSE(s.tryWait());
        ASSERT_FALSE(s.wait(10));

        flag = true;

        s.wait();
    });

    std::thread t2([&s, &flag]() {
        while (!flag)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

        s.signal(2);

        s.wait();
    });

    t1.join();
    t2.join();
}
