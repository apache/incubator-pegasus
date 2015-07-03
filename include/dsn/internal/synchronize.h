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
# pragma once

# include <dsn/internal/dsn_types.h>
# include <dsn/internal/logging.h>

# if 0

# include <mutex>
# include <condition_variable>
# include <boost/thread/shared_mutex.hpp>

namespace dsn { namespace utils {

class ex_lock
{
public:
    __inline void lock() { _lock.lock(); }
    __inline bool try_lock() { return _lock.try_lock(); }
    __inline void unlock() { _lock.unlock(); }
private:
    std::recursive_mutex _lock;
};

class rw_lock
{
public:
    rw_lock() { }
    ~rw_lock() {}

    void lock_read() { _lock.lock_shared(); }
    void unlock_read() { _lock.unlock_shared(); }

    void lock_write() { _lock.lock(); }
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

    inline void wait()
    {
        wait(TIME_MS_MAX);
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

}} // end namespace


# else
// using high performance versions from https://github.com/preshing/cpp11-on-multicore

# include <benaphore.h>
# include <autoresetevent.h>
# include <rwlock.h>

namespace dsn {
    namespace utils {

# if defined(_WIN32)
        class ex_lock
        {
        public:
            ex_lock() { ::InitializeCriticalSection(&_cs); }
            ~ex_lock() { ::DeleteCriticalSection(&_cs); }
            __inline void lock() { ::EnterCriticalSection(&_cs); }
            __inline bool try_lock() { return ::TryEnterCriticalSection(&_cs) != 0; }
            __inline void unlock() { ::LeaveCriticalSection(&_cs); }
        private:
            CRITICAL_SECTION _cs;
        };
# else
        class ex_lock
        {
        public:
            __inline void lock() { _lock.lock(); }
            __inline bool try_lock() { return _lock.tryLock(); }
            __inline void unlock() { _lock.unlock(); }
        private:
            RecursiveBenaphore _lock;
        };
# endif

        class rw_lock
        {
        public:
            rw_lock() { }
            ~rw_lock() {}

            __inline void lock_read() { _lock.lockReader(); }
            __inline void unlock_read() { _lock.unlockReader(); }

            __inline void lock_write() { _lock.lockWriter(); }
            __inline void unlock_write() { _lock.unlockWriter(); }

        private:
            NonRecursiveRWLock _lock;
        };

        class notify_event
        {
        public:
            __inline void notify() { _ready.signal(); }
            __inline void wait()   { _ready.wait(); }
            __inline bool wait_for(int milliseconds) 
            {
                if (TIME_MS_MAX == milliseconds)
                {
                    _ready.wait();
                    return true;
                }
                else
                    return _ready.wait(milliseconds); 
            }

        private:
            AutoResetEvent _ready;
        };
        
        class semaphore
        {
        public:
            semaphore(int initialCount = 0)
                : _sema(initialCount)
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
                _sema.signal(count);
            }

            inline void wait()
            {
                return _sema.wait();
            }

            inline bool wait(int milliseconds)
            {
                if (TIME_MS_MAX == milliseconds)
                {
                    _sema.wait();
                    return true;
                }
                else
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
    }
}

# endif


//--------------------- helpers --------------------------------------

namespace dsn {
    namespace utils {
        class auto_lock
        {
        public:
            auto_lock(ex_lock & lock) : _lock(&lock) { _lock->lock(); }
            ~auto_lock() { _lock->unlock(); }

        private:
            ex_lock * _lock;

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
    }
}
