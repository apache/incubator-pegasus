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

#include <dsn/utility/transient_memory.h>

namespace dsn {

typedef void *(*t_allocate)(size_t);
typedef void (*t_deallocate)(void *);

/// callocate_object overrides the operator "new" and "delete", which may be useful for objects
/// who want to use custom new/delete to boost performance
template <t_allocate a, t_deallocate d>
class callocator_object
{
public:
    void *operator new(size_t size) { return a(size); }

    void operator delete(void *p) { d(p); }

    void *operator new[](size_t size) { return a(size); }

    void operator delete[](void *p) { d(p); }
};

/// transient_object uses tls_trans_malloc/tls_trans_free as custom memory allocate.
/// in rdsn, serveral frequenctly allocated objects(task, rpc_message, etc.)
/// are derived from transient_objects,
/// so that their memory can be mamanged by trans_memory_allocator
typedef callocator_object<tls_trans_malloc, tls_trans_free> transient_object;
}
