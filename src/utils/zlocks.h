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

#include <algorithm>
#include <atomic>

#include "utils/utils.h"
#include "utils/ports.h"

///
/// synchronization objects of rDSN.
///
/// you MUST always use these objects to do synchronization when you write code
/// in rdsn's "service_app", because different implementations may be provided
/// when then program is running in different mode(nativerun/simulator).
///
/// As for the synchronize objects in "utils/synchronize.h", they are
/// used for synchronization inner the rdsn core runtime.
///

namespace dsn {
class ilock;
class zlock
{
public:
    zlock(bool recursive = false);
    ~zlock();

    void lock();
    bool try_lock();
    void unlock();

private:
    DISALLOW_COPY_AND_ASSIGN(zlock);
    ilock *_h;
};

class rwlock_nr_provider;
class zrwlock_nr
{
public:
    zrwlock_nr();
    ~zrwlock_nr();

    void lock_read();
    void unlock_read();
    bool try_lock_read();

    void lock_write();
    void unlock_write();
    bool try_lock_write();

private:
    DISALLOW_COPY_AND_ASSIGN(zrwlock_nr);
    rwlock_nr_provider *_h;
};

class semaphore_provider;
class zsemaphore
{
public:
    zsemaphore(int initial_count = 0);
    ~zsemaphore();

    void signal(int count = 1);
    bool wait(int timeout_milliseconds = TIME_MS_MAX);

private:
    DISALLOW_COPY_AND_ASSIGN(zsemaphore);
    semaphore_provider *_h;
};

class zevent
{
public:
    zevent(bool manualReset, bool initState = false);
    ~zevent();

    void set();
    void reset();
    bool wait(int timeout_milliseconds = TIME_MS_MAX);

private:
    DISALLOW_COPY_AND_ASSIGN(zevent);
    zsemaphore _sema;
    std::atomic<bool> _signaled;
    bool _manualReset;
};
} // namespace dsn

///
/// RAII wrapper of rdsn's synchronization objects
///
namespace dsn {
class zauto_lock
{
public:
    zauto_lock() : _locked(false), _lock(nullptr) {}
    zauto_lock(zlock &lock) : _locked(true), _lock(&lock) { _lock->lock(); }
    ~zauto_lock()
    {
        if (_locked) {
            _lock->unlock();
            _locked = false;
        }
    }

    void swap(zauto_lock &other)
    {
        std::swap(_locked, other._locked);
        std::swap(_lock, other._lock);
    }

private:
    bool _locked;
    zlock *_lock;
};

class zauto_read_lock
{
public:
    zauto_read_lock() : _locked(false), _lock(nullptr) {}
    zauto_read_lock(zrwlock_nr &lock) : _locked(true), _lock(&lock) { _lock->lock_read(); }
    ~zauto_read_lock()
    {
        if (_locked) {
            _lock->unlock_read();
            _locked = false;
        }
    }

    void swap(zauto_read_lock &other)
    {
        std::swap(_locked, other._locked);
        std::swap(_lock, other._lock);
    }

private:
    bool _locked;
    zrwlock_nr *_lock;
};

class zauto_write_lock
{
public:
    zauto_write_lock() : _locked(false), _lock(nullptr) {}
    zauto_write_lock(zrwlock_nr &lock) : _locked(true), _lock(&lock) { _lock->lock_write(); }
    ~zauto_write_lock()
    {
        if (_locked) {
            _lock->unlock_write();
            _locked = false;
        }
    }

    void swap(zauto_write_lock &other)
    {
        std::swap(_locked, other._locked);
        std::swap(_lock, other._lock);
    }

private:
    bool _locked;
    zrwlock_nr *_lock;
};
} // namespace dsn

///
/// utils function used to check the lock safety
///
namespace dsn {
namespace lock_checker {
void check_wait_safety();
void check_dangling_lock();
} // namespace lock_checker
} // namespace dsn
