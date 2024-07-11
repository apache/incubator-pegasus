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

#pragma once

#include "utils/synchronize.h"
#include "utils/zlock_provider.h"

namespace dsn {
namespace tools {

class std_lock_provider : public lock_provider
{
public:
    std_lock_provider(lock_provider *inner_provider) : lock_provider(inner_provider) {}
    virtual ~std_lock_provider() {}
    virtual void lock() { _lock.lock(); }
    virtual bool try_lock() { return _lock.try_lock(); }
    virtual void unlock() { _lock.unlock(); }

private:
    utils::ex_lock _lock;
};

class std_lock_nr_provider : public lock_nr_provider
{
public:
    std_lock_nr_provider(lock_nr_provider *inner_provider) : lock_nr_provider(inner_provider) {}
    virtual ~std_lock_nr_provider() {}
    virtual void lock() { _lock.lock(); }
    virtual bool try_lock() { return _lock.try_lock(); }
    virtual void unlock() { _lock.unlock(); }

private:
    utils::ex_lock_nr _lock;
};

class std_rwlock_nr_provider : public rwlock_nr_provider
{
public:
    std_rwlock_nr_provider(rwlock_nr_provider *inner_provider) : rwlock_nr_provider(inner_provider)
    {
    }
    virtual ~std_rwlock_nr_provider() {}
    virtual void lock_read() { _lock.lock_read(); }
    virtual void unlock_read() { _lock.unlock_read(); }
    virtual bool try_lock_read() { return _lock.try_lock_read(); }

    virtual void lock_write() { _lock.lock_write(); }
    virtual void unlock_write() { _lock.unlock_write(); }
    virtual bool try_lock_write() { return _lock.try_lock_write(); }

private:
    utils::rw_lock_nr _lock;
};

class std_semaphore_provider : public semaphore_provider
{
public:
    std_semaphore_provider(int initial_count, semaphore_provider *inner_provider)
        : semaphore_provider(initial_count, inner_provider), _sema(initial_count)
    {
    }

public:
    virtual ~std_semaphore_provider() {}
    virtual void signal(int count) { _sema.signal(count); }
    virtual bool wait(int timeout_milliseconds) { return _sema.wait(timeout_milliseconds); }

private:
    dsn::utils::semaphore _sema;
};
} // namespace tools
} // namespace dsn
