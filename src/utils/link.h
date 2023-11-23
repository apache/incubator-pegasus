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

#include <cassert>

// single linked list.
//
// assuming public T::T* next; exists and inited to nullptr in T::T(...)
//
template <typename T>
class slist
{
public:
    slist() { _first = _last = nullptr; }

    void add(T *obj)
    {
        if (_last) {
            _last->next = obj;
            _last = obj;
        } else {
            _first = _last = obj;
        }
    }

    T *pop_all()
    {
        T *ret = _first;
        _first = _last = nullptr;
        return ret;
    }

    T *pop_batch(/*inout*/ int &batch_size)
    {
        int c = 0;
        T *next = _first;
        while (next) {
            if (++c >= batch_size)
                break;

            next = next->next;
        }

        // all returned
        batch_size = c;

        if (next == nullptr || next == _last) {
            T *ret = _first;
            _first = _last = nullptr;
            return ret;
        }

        // partially returned
        else {
            // next is included
            T *ret = _first;
            _first = next->next;
            next->next = nullptr;
            return ret;
        }
    }

    T *pop_one()
    {
        if (_first) {
            T *ret = _first;

            if (_first == _last)
                _first = _last = nullptr;
            else
                _first = static_cast<T *>(_first->next);

            ret->next = nullptr;
            return ret;
        } else
            return nullptr;
    }

    bool is_empty() const { return _first == nullptr; }

public:
    T *_first;
    T *_last;
};

// double linked list.
class dlink
{
public:
    dlink() { _next = _prev = (this); }
    dlink *next() const { return _next; }
    dlink *prev() const { return _prev; }
    bool is_alone() const { return _next == this; }

    // insert me before existing link node o [p (this) o]
    void insert_before(dlink *o)
    {
        assert(is_alone()); //, "must not be linked to other list before insert");

        auto p = o->_prev;

        this->_next = o;
        o->_prev = (this);

        p->_next = this;
        this->_prev = p;
    }

    // insert me after existing link node o [o (this) n]
    void insert_after(dlink *o)
    {
        assert(is_alone()); //, "must not be linked to other list before insert");

        auto n = o->_next;

        this->_prev = o;
        o->_next = this;

        this->_next = n;
        n->_prev = this;
    }

    dlink *remove()
    {
        if (!is_alone()) {
            this->_next->_prev = this->_prev;
            this->_prev->_next = this->_next;
            _next = _prev = this;
        }
        return (this);
    }

    dlink *remove_and_get_next()
    {
        if (!is_alone()) {
            auto next = this->_next;
            this->_next->_prev = this->_prev;
            this->_prev->_next = this->_next;
            _next = _prev = this;
            return next;
        } else
            return nullptr;
    }

    /*
     *   BEFORE range_remove:
     *    this <=> [from <=> ... <=> to] <=> x ...
     *
     *   AFTER range_remove:
     *    this <=> x ...
     *    from <=> ... <=> to <=> from
     *
     *    return from;
     *
     *   caller must ensure *to* is valid
     */
    dlink *range_remove(dlink *to)
    {
        auto from = this->next();
        auto x = to->next();

        this->_next = x;
        x->_prev = this;

        to->_next = from;
        from->_prev = to;

        return from;
    }

private:
    dlink *_next;
    dlink *_prev;
};
