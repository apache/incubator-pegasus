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

#include "utils/ports.h"
#include "utils/utils.h"
#include "utils/hpc_locks/benaphore.h"
#include "utils/hpc_locks/autoresetevent.h"
#include "utils/hpc_locks/rwlock.h"

namespace dsn {
namespace utils {

class ex_lock
{
public:
    __inline void lock() { _lock.lock(); }
    __inline bool try_lock() { return _lock.tryLock(); }
    __inline void unlock() { _lock.unlock(); }

private:
    RecursiveBenaphore _lock;
};

class ex_lock_nr
{
public:
    __inline void lock() { _lock.lock(); }
    __inline bool try_lock() { return _lock.tryLock(); }
    __inline void unlock() { _lock.unlock(); }

private:
    NonRecursiveBenaphore _lock;
};

class ex_lock_nr_spin
{
public:
    __inline ex_lock_nr_spin() { _l = 0; }

    __inline void lock()
    {
        while (!try_lock()) {
            while (_l.load(std::memory_order_consume) == 1) {
            }
        }
    }

    __inline bool try_lock() { return 0 == _l.exchange(1, std::memory_order_acquire); }

    __inline void unlock() { _l.store(0, std::memory_order_release); }

private:
    std::atomic<int> _l;
};

class rw_lock_nr
{
public:
    rw_lock_nr() {}
    ~rw_lock_nr() {}

    __inline void lock_read() { _lock.lockReader(); }
    __inline void unlock_read() { _lock.unlockReader(); }
    __inline bool try_lock_read() { return _lock.tryLockReader(); }

    __inline void lock_write() { _lock.lockWriter(); }
    __inline void unlock_write() { _lock.unlockWriter(); }
    __inline bool try_lock_write() { return _lock.tryLockWriter(); }

private:
    NonRecursiveRWLock _lock;
};

class notify_event
{
public:
    __inline void notify() { _ready.signal(); }
    __inline void wait() { _ready.wait(); }
    __inline bool wait_for(int milliseconds)
    {
        if (TIME_MS_MAX == static_cast<unsigned int>(milliseconds)) {
            _ready.wait();
            return true;
        } else
            return _ready.wait(milliseconds);
    }

private:
    AutoResetEvent _ready;
};

class semaphore
{
public:
    semaphore(int initial_count = 0) : _sema(initial_count, 128) {}

    ~semaphore() {}

public:
    inline void signal() { signal(1); }

    inline void signal(int count) { _sema.signal(count); }

    inline void wait() { _sema.wait(); }

    inline bool wait(int milliseconds)
    {
        if (TIME_MS_MAX == static_cast<unsigned int>(milliseconds)) {
            _sema.wait();
            return true;
        } else
            return _sema.wait(milliseconds);
    }

    inline bool release()
    {
        _sema.signal();
        return true;
    }

private:
    LightweightSemaphore _sema;
};

//--------------------- helpers --------------------------------------
template <typename T>
class auto_lock
{
public:
    auto_lock(T &lock) : _lock(&lock) { _lock->lock(); }
    ~auto_lock() { _lock->unlock(); }

private:
    T *_lock;

    auto_lock(const auto_lock &);
    auto_lock &operator=(const auto_lock &);
};

class auto_read_lock
{
public:
    auto_read_lock(rw_lock_nr &lock) : _lock(&lock) { _lock->lock_read(); }
    ~auto_read_lock() { _lock->unlock_read(); }

private:
    rw_lock_nr *_lock;
};

class auto_write_lock
{
public:
    auto_write_lock(rw_lock_nr &lock) : _lock(&lock) { _lock->lock_write(); }
    ~auto_write_lock() { _lock->unlock_write(); }

private:
    rw_lock_nr *_lock;
};
} // namespace utils
} // namespace dsn
