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
 *     join point definition for various prototypes
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/utility/extensible_object.h>

namespace dsn {

class join_point_base
{
public:
    join_point_base(const char *name);

    bool put_front(void *fn, const char *name, bool is_native = false);
    bool put_back(void *fn, const char *name, bool is_native = false);
    bool put_before(const char *base, void *fn, const char *name, bool is_native = false);
    bool put_after(const char *base, void *fn, const char *name, bool is_native = false);
    bool remove(const char *name);
    bool put_replace(const char *base, void *fn, const char *name);

    const char *name() const { return _name.c_str(); }

protected:
    struct advice_entry
    {
        std::string name;
        void *func;
        bool is_native;
        advice_entry *next;
        advice_entry *prev;
    };

    advice_entry _hdr;
    std::string _name;

private:
    advice_entry *new_entry(void *fn, const char *name, bool is_native);
    advice_entry *get_by_name(const char *name);
};

struct join_point_unused_type
{
};

template <typename TReturn = void,
          typename T1 = join_point_unused_type,
          typename T2 = join_point_unused_type,
          typename T3 = join_point_unused_type>
class join_point;

template <typename TReturn, typename T1, typename T2, typename T3>
class join_point : public join_point_base
{
public:
    typedef TReturn (*point_prototype)(T1, T2, T3);
    typedef void (*advice_prototype)(T1, T2, T3);

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    TReturn execute(T1 p1, T2 p2, T3 p3, TReturn default_return_value)
    {
        TReturn returnValue = default_return_value;
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                returnValue = (*(point_prototype *)&p->func)(p1, p2, p3);
            } else {
                (*(advice_prototype *)&p->func)(p1, p2, p3);
            }
            p = p->next;
        }
        return returnValue;
    }
};

template <typename T1, typename T2, typename T3>
class join_point<void, T1, T2, T3> : public join_point_base
{
public:
    typedef void (*point_prototype)(T1, T2, T3);
    typedef void (*advice_prototype)(T1, T2, T3);

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    void execute(T1 p1, T2 p2, T3 p3)
    {
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                (*(point_prototype *)&p->func)(p1, p2, p3);
            } else {
                (*(advice_prototype *)&p->func)(p1, p2, p3);
            }
            p = p->next;
        }
    }
};

template <typename TReturn, typename T1, typename T2>
class join_point<TReturn, T1, T2, join_point_unused_type> : public join_point_base
{
public:
    typedef TReturn (*point_prototype)(T1, T2);
    typedef void (*advice_prototype)(T1, T2);

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    TReturn execute(T1 p1, T2 p2, TReturn default_return_value)
    {
        TReturn returnValue = default_return_value;
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                returnValue = (*(point_prototype *)&p->func)(p1, p2);
            } else {
                (*(advice_prototype *)&p->func)(p1, p2);
            }
            p = p->next;
        }
        return returnValue;
    }
};

template <typename T1, typename T2>
class join_point<void, T1, T2, join_point_unused_type> : public join_point_base
{
public:
    typedef void (*point_prototype)(T1, T2);
    typedef void (*advice_prototype)(T1, T2);

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    void execute(T1 p1, T2 p2)
    {
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                (*(point_prototype *)&p->func)(p1, p2);
            } else {
                (*(advice_prototype *)&p->func)(p1, p2);
            }
            p = p->next;
        }
    }
};

template <typename TReturn, typename T1>
class join_point<TReturn, T1, join_point_unused_type, join_point_unused_type>
    : public join_point_base
{
public:
    typedef TReturn (*point_prototype)(T1);
    typedef void (*advice_prototype)(T1);

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    TReturn execute(T1 p1, TReturn default_return_value)
    {
        TReturn returnValue = default_return_value;
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                returnValue = (*(point_prototype *)&p->func)(p1);
            } else {
                (*(advice_prototype *)&p->func)(p1);
            }
            p = p->next;
        }
        return returnValue;
    }
};

template <typename T1>
class join_point<void, T1, join_point_unused_type, join_point_unused_type> : public join_point_base
{
public:
    typedef void (*point_prototype)(T1);
    typedef void (*advice_prototype)(T1);

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    void execute(T1 p1)
    {
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                (*(point_prototype *)&p->func)(p1);
            } else {
                (*(advice_prototype *)&p->func)(p1);
            }
            p = p->next;
        }
    }
};

template <typename TReturn>
class join_point<TReturn, join_point_unused_type, join_point_unused_type, join_point_unused_type>
    : public join_point_base
{
public:
    typedef TReturn (*point_prototype)();
    typedef void (*advice_prototype)();

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    TReturn execute(TReturn default_return_value)
    {
        TReturn returnValue = default_return_value;
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                returnValue = (*(point_prototype *)&p->func)();
            } else {
                (*(advice_prototype *)&p->func)();
            }
            p = p->next;
        }
        return returnValue;
    }
};

template <>
class join_point<void, join_point_unused_type, join_point_unused_type, join_point_unused_type>
    : public join_point_base
{
public:
    typedef void (*point_prototype)();
    typedef void (*advice_prototype)();

public:
    join_point(const char *name) : join_point_base(name) {}
    bool put_native(point_prototype point)
    {
        return join_point_base::put_front((void *)point, "native", true);
    }
    bool put_front(advice_prototype fn, const char *name)
    {
        return join_point_base::put_front((void *)fn, name);
    }
    bool put_back(advice_prototype fn, const char *name)
    {
        return join_point_base::put_back((void *)fn, name);
    }
    bool put_before(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_before(base, (void *)fn, name);
    }
    bool put_after(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_after(base, (void *)fn, name);
    }
    bool put_replace(const char *base, advice_prototype fn, const char *name)
    {
        return join_point_base::put_replace(base, (void *)fn, name);
    }

    void execute()
    {
        advice_entry *p = _hdr.next;
        while (p != &_hdr) {
            if (p->is_native) {
                (*(point_prototype *)&p->func)();
            } else {
                (*(advice_prototype *)&p->func)();
            }
            p = p->next;
        }
    }
};

} // end namespace dsn
