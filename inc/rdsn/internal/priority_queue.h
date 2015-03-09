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
        _peeked_item = nullptr;
    }

    virtual long enqueue(T obj, uint32_t priority)
    {
        rassert (priority >= 0 && priority < priority_count, "wrong priority");

        std::lock_guard<std::mutex>  l(_lock);
        {
            _items[priority].push(obj);
            return ++_count;
        }
    }

    virtual T peek()
    {
        std::lock_guard<std::mutex>  l(_lock);

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
        std::lock_guard<std::mutex>  l(_lock);
        auto c = _peeked_item;
        _peeked_item = nullptr;
        return c;
    }

    bool is_peeked()
    {
        std::lock_guard<std::mutex>  l(_lock);
        return _peeked_item != nullptr;
    }

    virtual T dequeue()
    {
        std::lock_guard<std::mutex>  l(_lock);
        long ct = 0;
        return dequeue_impl(ct);
    }

    virtual T dequeue(__out long& ct)
    {
        std::lock_guard<std::mutex>  l(_lock);
        return dequeue_impl(ct);
    }
    
    const std::string& get_name() const { return _name;}

    long count() const { std::lock_guard<std::mutex>  l(_lock); return _count; }

protected:
    T dequeue_impl(__out long& ct, bool pop = true)
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

        rassert(index >= 0, "must find something");
        auto c = _items[index].front();
        _items[index].pop();
        return c;
    }

protected:
    std::string   _name;
    T             _peeked_item;
    TQueue        _items[priority_count];
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
        _wait_count = 0;
    }

    virtual long enqueue(T obj, uint32_t priority)
    {
        long r;
        std::lock_guard<std::mutex> l(priority_queue<T, priority_count, TQueue>::_lock);
        
        priority_queue<T, priority_count, TQueue>::_items[priority].push(obj);
        r = ++priority_queue<T, priority_count, TQueue>::_count;
        
        if (_wait_count > 0)
        {
            _cond.notify_one();
        }
        
        return r;
    }

    virtual T dequeue(__out long ct, int millieseconds = INFINITE)
    {
        std::unique_lock<std::mutex> l(priority_queue<T, priority_count, TQueue>::_lock);
        
        if (priority_queue<T, priority_count, TQueue>::_count > 0)
        {
            return priority_queue<T, priority_count, TQueue>::dequeue_impl(ct);
        }

        ++_wait_count;
        if (millieseconds == INFINITE)
        {
            _cond.wait(l);
        }
        else
        {
            _cond.wait_for(l, std::chrono::milliseconds(millieseconds));
        }
        --_wait_count;

        return priority_queue<T, priority_count, TQueue>::dequeue_impl(ct);
    }
    
private:
    std::condition_variable _cond;
    int                     _wait_count;
};

}} // end namespace
