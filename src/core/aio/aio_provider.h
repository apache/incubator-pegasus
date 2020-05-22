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
 *     define the interface for disk operation provider
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/tool-api/task.h>
#include <dsn/utility/dlib.h>

namespace dsn {

class disk_engine;
class service_node;
class task_worker_pool;
class task_queue;

#if defined(_WIN32)
#define DSN_INVALID_FILE_HANDLE ((dsn_handle_t)(uintptr_t)0)
#else
#define DSN_INVALID_FILE_HANDLE ((dsn_handle_t)(uintptr_t)-1)
#endif

/*!
@addtogroup tool-api-providers
@{
*/
//
// !!! all threads must be started with task::set_tls_dsn_context(provider->node(), null);
//
class aio_provider
{
public:
    template <typename T>
    static aio_provider *create(disk_engine *disk, aio_provider *inner_provider)
    {
        return new T(disk, inner_provider);
    }

    typedef aio_provider *(*factory)(disk_engine *, aio_provider *);

public:
    DSN_API aio_provider(disk_engine *disk, aio_provider *inner_provider);
    virtual ~aio_provider() {}
    DSN_API service_node *node() const;

    // return DSN_INVALID_FILE_HANDLE if failed
    // TODO(wutao1): return uint64_t instead (because we only support linux now)
    virtual dsn_handle_t open(const char *file_name, int flag, int pmode) = 0;

    virtual error_code close(dsn_handle_t fh) = 0;
    virtual error_code flush(dsn_handle_t fh) = 0;

    // Submits the aio_task to the underlying disk-io executor.
    // This task may not be executed immediately, call `aio_task::wait`
    // to wait until it completes.
    // TODO(wutao1): Call it aio_submit().
    virtual void aio(aio_task *aio) = 0;

    virtual aio_context *prepare_aio_context(aio_task *) = 0;

protected:
    DSN_API void
    complete_io(aio_task *aio, error_code err, uint32_t bytes, int delay_milliseconds = 0);

private:
    disk_engine *_engine;
};

/*@}*/
} // namespace dsn
