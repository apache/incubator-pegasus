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

#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/lockp.std.h"

using namespace dsn;
using namespace dsn::tools;

TEST(tools_common, std_lock_provider)
{
    std_lock_provider *lock = new std_lock_provider(nullptr);
    lock->lock();
    EXPECT_TRUE(lock->try_lock());
    lock->unlock();
    lock->unlock();

    std_lock_nr_provider *nr_lock = new std_lock_nr_provider(nullptr);
    nr_lock->lock();
    EXPECT_FALSE(nr_lock->try_lock());
    nr_lock->unlock();

    std_rwlock_nr_provider *rwlock = new std_rwlock_nr_provider(nullptr);
    rwlock->lock_read();
    rwlock->unlock_read();
    rwlock->lock_write();
    rwlock->unlock_write();

    std_semaphore_provider *sema = new std_semaphore_provider(0, nullptr);
    std::thread t([](std_semaphore_provider *s) { s->wait(1000000); }, sema);
    sema->signal(1);
    t.join();

    delete lock;
    delete nr_lock;
    delete rwlock;
    delete sema;
}
