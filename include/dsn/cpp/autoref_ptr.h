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

# include <atomic>

namespace dsn
{
    class ref_counter
    {
    public:
        ref_counter()
        {
            _counter = 0;
        }

        virtual ~ref_counter()
        {
        }

        void add_ref()
        {
            ++_counter;
        }

        void release_ref()
        {
            if (--_counter == 0)
                delete this;
        }

        long get_count()
        {
            return _counter.load();
        }

    private:
        std::atomic<long> _counter;
    };

    template<typename T>  // T : ref_counter
    class ref_ptr
    {
    public:
        ref_ptr() : _obj(nullptr)
        {
        }

        ref_ptr(T* obj) : _obj(obj)
        {
            if (nullptr != _obj)
                _obj->add_ref();
        }

        ref_ptr(const ref_ptr<T>& r)
        {
            _obj = r.get();
            if (nullptr != _obj)
                _obj->add_ref();
        }

        ref_ptr(ref_ptr<T>&& r)
            : _obj(r._obj)
        {
            r._obj = nullptr;
        }

        ~ref_ptr()
        {
            if (nullptr != _obj)
            {
                _obj->release_ref();
            }
        }

        ref_ptr<T>& operator = (T* obj)
        {
            if (_obj == obj)
                return *this;

            if (nullptr != _obj)
            {
                _obj->release_ref();
            }

            _obj = obj;

            if (obj != nullptr)
            {
                _obj->add_ref();
            }

            return *this;
        }

        ref_ptr<T>& operator = (const ref_ptr<T>& obj)
        {
            return operator = (obj._obj);
        }

        ref_ptr<T>& operator = (ref_ptr<T>&& obj)
        {
            if (nullptr != _obj)
            {
                _obj->release_ref();
            }

            _obj = obj._obj;
            obj._obj = nullptr;
            return *this;
        }

        T* get() const
        {
            return _obj;
        }

        operator T* () const
        {
            return _obj;
        }

        T& operator * () const
        {
            return (*_obj);
        }

        T* operator -> () const
        {
            return _obj;
        }

        bool operator == (T* r) const
        {
            return _obj == r;
        }

        bool operator != (T* r) const
        {
            return _obj != r;
        }

    private:
        T* _obj;
    };

} // end namespace dsn
