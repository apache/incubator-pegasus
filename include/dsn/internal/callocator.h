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

# include <memory>

namespace dsn {

    typedef void* (*t_allocate)(size_t);
    typedef void  (*t_deallocate)(void*);

    template <t_allocate a, t_deallocate d>
    class callocator_object
    {
    public:
        void* operator new(size_t size)
        {
            return a(size);
        }

        void operator delete(void* p)
        {
            d(p);
        }

        void* operator new[](size_t size)
        {
            return a(size);
        }

        void operator delete[](void* p)
        {
            d(p);
        }
    };

    template <typename T, t_allocate a, t_deallocate d>
    class callocator : public std::allocator<T>
    {
    public:
        typedef size_t size_type;
        typedef T* pointer;
        typedef const T* const_pointer;

        template<typename _Tp1>
        struct rebind
        {
            typedef callocator<_Tp1, a, d> other;
        };

        pointer allocate(size_type n, const void *hint = 0)
        {
            return a(n);
        }

        void deallocate(pointer p, size_type n)
        {
            return d(p);
        }

        callocator() throw() : std::allocator<T>() { }
        callocator(const callocator<T, a, d> &ac) throw() : std::allocator<T>(ac) { }
        ~callocator() throw() { }
    };
}
