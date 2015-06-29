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
        static std::once_flag flag;

        if (nullptr == _instance)
        {
            std::call_once(flag, [&]() 
            { 
                auto tmp = new T();
                std::atomic_thread_fence(std::memory_order::memory_order_seq_cst);
                _instance = tmp; 
            });
        }
        return *_instance;
    }

    static bool is_instance_created()
    {
        return nullptr != _instance;
    }
    
protected:
    static T*    _instance;
    
private:
    singleton(const singleton&);
    singleton& operator=(const singleton&);
};

// ----- inline implementations -------------------------------------------------------------------

template<typename T> T*    singleton<T>::_instance = 0;

}} // end namespace dsn::utils
