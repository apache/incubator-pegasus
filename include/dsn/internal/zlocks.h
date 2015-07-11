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

#include <dsn/internal/zlock_provider.h>
#include <atomic>

namespace dsn { namespace service {

namespace lock_checker {
    extern __thread int zlock_exclusive_count;
    extern __thread int zlock_shared_count;
    extern void check_wait_safety();
    extern void check_dangling_lock();
    extern void check_wait_task(task* waitee);
}

class zlock
{
public:
    zlock();
    ~zlock();

    void lock() { _provider->lock(); lock_checker::zlock_exclusive_count++;  }
    bool try_lock() { auto r = _provider->try_lock(); if (r) lock_checker::zlock_exclusive_count++;  return r; }
    void unlock() { lock_checker::zlock_exclusive_count--; _provider->unlock(); }

private:
    dsn::lock_provider *_provider;

private:
    // no assignment operator
    zlock& operator=(const zlock& source);
    zlock(const zlock& source);
};

class zrwlock_nr
{
public:
    zrwlock_nr();
    ~zrwlock_nr();

    void lock_read() { _provider->lock_read(); lock_checker::zlock_shared_count++;  }
    void unlock_read() { lock_checker::zlock_shared_count--; _provider->unlock_read(); }

    void lock_write() { _provider->lock_write(); lock_checker::zlock_exclusive_count++; }
    void unlock_write() { lock_checker::zlock_exclusive_count--; _provider->unlock_write(); }

private:
    dsn::rwlock_nr_provider *_provider;

private:
    // no assignment operator
    zrwlock_nr& operator=(const zrwlock_nr& source);
    zrwlock_nr(const zrwlock_nr& source);
};

class zsemaphore
{
public:  
    zsemaphore(int initialCount = 0);
    ~zsemaphore();

public:
    virtual void signal(int count = 1) { _provider->signal(count); }

    virtual bool wait(int timeout_milliseconds = TIME_MS_MAX) { lock_checker::check_wait_safety();  return _provider->wait(timeout_milliseconds); }

private:
    dsn::semaphore_provider *_provider;

private:
    // no assignment operator
    zsemaphore& operator=(const zsemaphore& source);
    zsemaphore(const zsemaphore& source);
};

class zevent
{
public:
    zevent(bool manualReset, bool initState = false);
    ~zevent();

public:
    void set();
    void reset();
    bool wait(int timeout_milliseconds = TIME_MS_MAX);

private:
    zsemaphore        _sema;
    std::atomic<bool> _signaled;
    bool              _manualReset;

private:
    // no assignment operator
    zevent& operator=(const zevent& source);
    zevent(const zevent& source);
};

class zauto_lock
{
public:
    zauto_lock (zlock & lock) : _lock(&lock) { _lock->lock(); }
    ~zauto_lock() { _lock->unlock(); }

private:
    zlock * _lock; 
};

class zauto_read_lock
{
public:
    zauto_read_lock (zrwlock_nr & lock) : _lock(&lock) { _lock->lock_read(); }
    ~zauto_read_lock() { _lock->unlock_read(); }

private:
    zrwlock_nr * _lock; 
};

class zauto_write_lock
{
public:
    zauto_write_lock (zrwlock_nr & lock) : _lock(&lock) { _lock->lock_write(); }
    ~zauto_write_lock() { _lock->unlock_write(); }

private:
    zrwlock_nr * _lock; 
};

}} // end namespace dsn::service
