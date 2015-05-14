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

// class T : public slink<T>
template<typename T>
class slink
{
public:
    slink() : _next(nullptr){}

    T* next() const { return _next; }
    
    void insert_after(T* o) 
    {
        T* n = _next; 
        _next = o; 
        o->_next = n; 
    }

    T* remove_next() 
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
    T *_next;
};


// class T : public dlink<T>
template<typename T>
class dlink
{
public:
    dlink() { _next = _prev = dynamic_cast<T*>(this); }
    T* next() const { return _next; }
    T* prev() const { return _prev; }
    bool is_alone() const { return _next == _prev; }

    void insert_after(T* o)
    {
        auto n = _next;
        
        this->_next = o;
        o->_prev = dynamic_cast<T*>(this);

        o->_next = n;
        n->_prev = o;        
    }

    void insert_before(T* o)
    {
        auto n = _prev;

        this->_prev = o;
        o->_next = dynamic_cast<T*>(this);

        o->_prev = n;
        n->_next = o;
    }

    T* remove()
    {
        if (!is_alone())
        {
            this->_next->_prev = this->_prev;
            this->_prev->_next = this->_next;
        }
        return dynamic_cast<T*>(this);
    }

private:
    T* _next;
    T* _prev;
};
