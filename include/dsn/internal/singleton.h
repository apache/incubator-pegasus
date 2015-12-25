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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <mutex>
# include <atomic>

namespace dsn { namespace utils {

template<typename T>
class singleton
{
public:
    singleton() {}

    static T& instance()
    {
        if (nullptr == _instance)
        {
            // lock 
            while (0 != _l.exchange(1, std::memory_order_acquire))
            {
                while (_l.load(std::memory_order_consume) == 1)
                {
                }
            }

            // re-check and assign
            if (nullptr == _instance)
            {
                auto tmp = new T();
                std::atomic_thread_fence(std::memory_order::memory_order_seq_cst);
                _instance = tmp;
            }            

            // unlock
            _l.store(0, std::memory_order_release);
        }
        return *_instance;
    }

    static T& fast_instance()
    {
        return *_instance;
    }

    static bool is_instance_created()
    {
        return nullptr != _instance;
    }
    
protected:
    static T*    _instance;
    static std::atomic<int> _l;
    
private:
    singleton(const singleton&);
    singleton& operator=(const singleton&);
};

// ----- inline implementations -------------------------------------------------------------------

template<typename T> T*  singleton<T>::_instance = 0;
template<typename T> std::atomic<int>  singleton<T>::_l(0);

}} // end namespace dsn::utils
