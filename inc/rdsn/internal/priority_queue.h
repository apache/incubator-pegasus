#pragma once

#include <mutex>
#include <queue>
#include <cassert>
#include <condition_variable>
#include <rdsn/internal/logging.h>

namespace rdsn { namespace utils {

template<typename T, int priority_count, typename TQueue = std::queue<T>>
class priority_queue
{
public:    
    priority_queue(const std::string& name)
    {
        _name = name;
        _count = 0;
    }

    virtual long enqueue(T obj, uint32_t priority)
    {
        rdsn_assert (priority >= 0 && priority < priority_count, "wrong priority");

        std::lock_guard<std::mutex>  l(_lock);
        {
            _items[priority].push(obj);
            return ++_count;
        }
    }

    virtual T dequeue()
    {
        std::lock_guard<std::mutex>  l(_lock);
        {
            if (_count == 0)
            {
                return nullptr;
            }

            --_count;

            int index = priority_count - 1;
            for (; index >= 0; index--)
            {
                if (_items[index].size() > 0)
                {
                    break;
                }
            }

            rdsn_assert(index >= 0, "must find something");
            auto c = _items[index].front();
            _items[index].pop();
            return c;
        }
    }

    virtual T dequeue(__out long& ct)
    {
        std::lock_guard<std::mutex>  l(_lock);
        return dequeue_impl(ct);
    }
    
    const std::string& get_name() const { return _name;}

    long count() const { std::lock_guard<std::mutex>  l(_lock); return _count; }

protected:
    T dequeue_impl(__out long& ct)
    {

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

            rdsn_assert(index >= 0, "must find something");
            auto c = _items[index].front();
            _items[index].pop();
            return c;
        }
    }

private:
    std::string   _name;
    TQueue        _items[priority_count];

protected:
    long          _count;
    mutable std::mutex _lock;
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
        long r = priority_queue<T, priority_count, TQueue>::enqueue(obj, priority);
        _cond.notify_one();
        return r;
    }

    virtual T dequeue(__out long ct, int millieseconds = INFINITE)
    {
        std::unique_lock<std::mutex> l(priority_queue<T, priority_count, TQueue>::_lock);
        if (priority_queue<T, priority_count, TQueue>::_count > 0)
        {
            return priority_queue<T, priority_count, TQueue>::dequeue_impl(ct);
        }

        if (millieseconds == INFINITE)
        {
            _cond.wait(l);
        }
        else
        {
            _cond.wait_for(l, std::chrono::milliseconds(millieseconds));
        }

        return priority_queue<T, priority_count, TQueue>::dequeue_impl(ct);
    }
    
private:
    std::condition_variable _cond;
};

}} // end namespace
