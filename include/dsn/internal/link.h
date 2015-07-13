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

# include <dsn/internal/logging.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "linklist"

class slink
{
public:
    slink() : _next(nullptr){}

    slink* next() const { return _next; }
    
    void insert_after(slink* o) 
    {
        slink* n = _next; 
        _next = o; 
        o->_next = n; 
    }

    slink* remove_next() 
    {
        if (_next) 
        {
            auto n = _next; 
            _next = _next->_next; 
            return n; 
        }
        else 
            return nullptr; 
    }

private:
    slink *_next;
};

class dlink
{
public:
    dlink() { _next = _prev = (this); }
    dlink* next() const { return _next; }
    dlink* prev() const { return _prev; }
    bool is_alone() const { return _next == this; }

    // insert me before existing link node o [p (this) o]
    void insert_before(dlink* o)
    {
        dbg_dassert(is_alone(), "must not be linked to other list before insert");

        auto p = o->_prev;
        
        this->_next = o;
        o->_prev = (this);

        p->_next = this;
        this->_prev = p;
    }

    // insert me after existing link node o [o (this) n]
    void insert_after(dlink* o)
    {
        dbg_dassert(is_alone(), "must not be linked to other list before insert");

        auto n = o->_next;

        this->_prev = o;
        o->_next = this;

        this->_next = n;
        n->_prev = this;
    }

    dlink* remove()
    {
        if (!is_alone())
        {
            this->_next->_prev = this->_prev;
            this->_prev->_next = this->_next;
            _next = _prev = this;
        }
        return (this);
    }

private:
    dlink* _next;
    dlink* _prev;
};
