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

#include <queue>
#include <cassert>
#include <dsn/internal/logging.h>
#include <dsn/internal/synchronize.h>

namespace dsn { namespace utils {

template<typename T, int priority_count, typename TQueue = std::queue<T>>
class priority_queue
{
public:    
    priority_queue(const std::string& name)
    {
        _name = name;
        _count = 0;
        _peeked_item = nullptr;
    }

    virtual long enqueue(T obj, uint32_t priority)
    {
        dassert (priority >= 0 && priority < priority_count, "wrong priority");

        auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        {
            _items[priority].push(obj);
            return ++_count;
        }
    }

    virtual T peek()
    {
        auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);

        // already peeked
        if (nullptr != _peeked_item)
            return nullptr;

        else
        {
            long ct = 0;
            _peeked_item = dequeue_impl(ct);
            return _peeked_item;
        }
    }

    virtual T dequeue_peeked()
    {
        auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        auto c = _peeked_item;
        _peeked_item = nullptr;
        return c;
    }

    bool is_peeked()
    {
        auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        return _peeked_item != nullptr;
    }

    virtual T dequeue()
    {
        auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        long ct = 0;
        return dequeue_impl(ct);
    }

    virtual T dequeue(__out_param long& ct)
    {
        auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
        return dequeue_impl(ct);
    }
    
    const std::string& get_name() const { return _name;}

    long count() const { auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock); return _count; }

protected:
    T dequeue_impl(__out_param long& ct, bool pop = true)
    {
        if (_count == 0)
        {
            return nullptr;
        }

        ct = --_count;

        int index = priority_count - 1;
        for (; index >= 0; index--)
        {
            if (_items[index].size() > 0)
            {
                break;
            }
        }

        dassert (index >= 0, "must find something");
        auto c = _items[index].front();
        _items[index].pop();
        return c;
    }

protected:
    std::string   _name;
    T             _peeked_item;
    TQueue        _items[priority_count];
    long          _count;
    mutable utils::ex_lock_nr_spin _lock;
};

template<typename T, int priority_count, typename TQueue = std::queue<T>>
class blocking_priority_queue : public priority_queue<T, priority_count, TQueue>
{
public:
    blocking_priority_queue(const std::string& name)
        : priority_queue<T, priority_count, TQueue>(name)
    {
    }

    virtual long enqueue(T obj, uint32_t priority)
    { 
        auto r = priority_queue<T, priority_count, TQueue>::enqueue(obj, priority);
        _sema.signal();
        return r;
    }

    virtual T dequeue(__out_param long& ct, int millieseconds = TIME_MS_MAX)
    {
        _sema.wait();
        return priority_queue<T, priority_count, TQueue>::dequeue(ct);
    }
    
private:
    semaphore _sema;
};

}} // end namespace
