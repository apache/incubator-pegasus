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

#pragma once

#include <dsn/service_api_c.h>
#include <dsn/utility/ports.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/utility/error_code.h>
#include <dsn/tool-api/threadpool_code.h>
#include <dsn/tool-api/task_code.h>
#include <dsn/tool-api/gpid.h>
#include <memory>
#include <atomic>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

namespace dsn {
typedef void (*safe_handle_release)(void *);

template <safe_handle_release releaser>
class safe_handle : public ::dsn::ref_counter
{
public:
    safe_handle(void *handle, bool is_owner)
    {
        _handle = handle;
        _is_owner = is_owner;
    }

    safe_handle()
    {
        _handle = nullptr;
        _is_owner = false;
    }

    void assign(void *handle, bool is_owner)
    {
        clear();

        _handle = handle;
        _is_owner = is_owner;
    }

    void set_owner(bool owner = true) { _is_owner = owner; }

    ~safe_handle() { clear(); }

    void *native_handle() const { return _handle; }

private:
    void clear()
    {
        if (_is_owner && nullptr != _handle) {
            releaser(_handle);
            _handle = nullptr;
        }
    }

private:
    void *_handle;
    bool _is_owner;
};
}
