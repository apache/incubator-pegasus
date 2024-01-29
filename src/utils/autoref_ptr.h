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

#include <atomic>
#include <cassert>
#include <type_traits>
#include <utility>

namespace dsn {
class ref_counter
{
public:
    ref_counter() : _magic(0xdeadbeef), _counter(0) {}

    virtual ~ref_counter()
    {
        // 0xdeadbeef: 3735928559
        assert(_magic == 0xdeadbeef);

        // 0xfacedead: 4207861421
        _magic = 0xfacedead;
    }

    void add_ref()
    {
        // 0xdeadbeef: 3735928559
        assert(_magic == 0xdeadbeef);

        // Increasing the reference counter can always be done with memory_order_relaxed:
        // New references to an object can only be formed from an existing reference,
        // and passing an existing reference from one thread to another must already provide any
        // required synchronization.
        _counter.fetch_add(1, std::memory_order_relaxed);
    }

    void release_ref()
    {
        // 0xdeadbeef: 3735928559
        assert(_magic == 0xdeadbeef);

        // It is important to enforce any possible access to the object in one thread
        //(through an existing reference) to happen before deleting the object in a different
        // thread.
        // This is achieved by a "release" operation after dropping a reference
        //(any access to the object through this reference must obviously happened before),
        // and an "acquire" operation before deleting the object.
        // reference: http://www.boost.org/doc/libs/1_60_0/doc/html/atomic/usage_examples.html
        if (_counter.fetch_sub(1, std::memory_order_release) == 1) {
            std::atomic_thread_fence(std::memory_order_acquire);
            delete this;
        }
    }

    long get_count() const { return _counter.load(); }

protected:
    unsigned int _magic;
    std::atomic<long> _counter;

public:
    ref_counter(const ref_counter &) = delete;
    ref_counter &operator=(const ref_counter &) = delete;
};

template <typename T> // T : ref_counter
class ref_ptr
{
public:
    ref_ptr() : _obj(nullptr) {}

    ref_ptr(T *obj) : _obj(obj)
    {
        if (nullptr != _obj)
            _obj->add_ref();
    }

    ref_ptr(const ref_ptr<T> &r)
    {
        _obj = r.get();
        if (nullptr != _obj)
            _obj->add_ref();
    }

    template <typename U,
              typename = typename std::enable_if<std::is_convertible<U *, T *>::value>::type>
    ref_ptr(const ref_ptr<U> &r)
    {
        _obj = r.get();
        if (nullptr != _obj)
            _obj->add_ref();
    }

    ref_ptr(ref_ptr<T> &&r) : _obj(r._obj) { r._obj = nullptr; }

    template <typename U,
              typename = typename std::enable_if<std::is_convertible<U *, T *>::value>::type>
    ref_ptr(ref_ptr<U> &&r) noexcept : _obj(r._obj)
    {
        r._obj = nullptr;
    }

    ~ref_ptr()
    {
        if (nullptr != _obj) {
            _obj->release_ref();
        }
    }

    ref_ptr<T> &operator=(T *obj) { return *this = ref_ptr(obj); }

    ref_ptr<T> &operator=(ref_ptr<T> r) noexcept
    {
        swap(r);
        return *this;
    }

    template <typename U,
              typename = typename std::enable_if<std::is_convertible<U *, T *>::value>::type>
    ref_ptr<T> &operator=(ref_ptr<U> r) noexcept
    {
        ref_ptr<T> p(r);
        swap(p);
        return *this;
    }

    void swap(ref_ptr<T> &r) noexcept { std::swap(_obj, r._obj); }

    void reset(T *obj = nullptr) { *this = obj; }

    T *get() const { return _obj; }

    operator T *() const { return _obj; }

    T &operator*() const { return (*_obj); }

    T *operator->() const { return _obj; }

    bool operator==(T *r) const { return _obj == r; }

    bool operator!=(T *r) const { return _obj != r; }

private:
    T *_obj;

    template <typename U>
    friend class ref_ptr;
};

} // end namespace dsn
