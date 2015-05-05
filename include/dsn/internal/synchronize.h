/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus(rDSN) -=- 
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
 # pragma once

# include <dsn/internal/dsn_types.h>
# include <mutex>
# include <condition_variable>
# include <boost/thread/shared_mutex.hpp>
# include <dsn/internal/logging.h>

namespace dsn { namespace utils {

class rw_lock
{
public:
    rw_lock() { }
    ~rw_lock() {}

    void lock_read() { _lock.lock_shared(); }
    bool try_lock_read() { return _lock.try_lock_shared(); }
    void unlock_read() { _lock.unlock_shared(); }

    void lock_write() { _lock.lock(); }
    bool try_lock_write() { return _lock.try_lock(); }
    void unlock_write() { _lock.unlock(); }
    
private:
    boost::shared_mutex _lock;
};

class notify_event
{
public:
    notify_event() : _ready(false){}
    void notify() { std::lock_guard<std::mutex> l(_lk); _ready = true; _cond.notify_all(); }
    void wait()   { std::unique_lock<std::mutex> l(_lk); _cond.wait(l, [&]{return _ready; }); }
    bool wait_for(int milliseconds)
    { 
        std::unique_lock<std::mutex> l(_lk); 
        if (_ready)
        {
            return true;
        }
        else
        {
            return std::cv_status::no_timeout == _cond.wait_for(l, std::chrono::milliseconds(milliseconds));
        }
    }

private:
    std::mutex _lk;
    std::condition_variable _cond;
    bool _ready;
};


class semaphore
{
public:  
    semaphore(int initialCount = 0)
        : _count(initialCount), _waits(0)
    {
    }

    ~semaphore()
    {
    }

public:
    inline void signal() 
    {
        signal(1);
    }

    inline void signal(int count)
    {
        std::unique_lock<std::mutex> l(_lk);
        _count += count;
        if (_waits > 0)
        {
            _cond.notify_one();
        }
    }

    inline bool wait()
    {
        return wait(TIME_MS_MAX);
    }

    inline bool wait(unsigned int milliseconds)
    {
        std::unique_lock<std::mutex> l(_lk);
        if (_count == 0)
        {
            _waits++;

            auto r = _cond.wait_for(l, std::chrono::milliseconds(milliseconds));

            _waits--;
            if (r == std::cv_status::timeout)
                return false;
        }

        dassert (_count > 0, "semphoare must be greater than zero");
        _count--;
        return true;
    }

    inline bool release()
    {
        signal(1);
        return true;
    }    

private:
    std::mutex _lk;
    std::condition_variable _cond;
    int _count;
    int _waits;
};


//--------------------- helpers --------------------------------------

class auto_lock
{
public:
    auto_lock(std::recursive_mutex & lock) : _lock(&lock) { _lock->lock(); }
    ~auto_lock() { _lock->unlock(); }

private:
    std::recursive_mutex * _lock;

    auto_lock(const auto_lock&);
    auto_lock& operator=(const auto_lock&);
};

class auto_read_lock
{
public:
    auto_read_lock(rw_lock & lock) : _lock(&lock) { _lock->lock_read(); }
    ~auto_read_lock() { _lock->unlock_read(); }

private:
    rw_lock * _lock;
};

class auto_write_lock
{
public:
    auto_write_lock(rw_lock & lock) : _lock(&lock) { _lock->lock_write(); }
    ~auto_write_lock() { _lock->unlock_write(); }

private:
    rw_lock * _lock;
};

}} // end namespace
