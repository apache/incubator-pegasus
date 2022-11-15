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

#include "aio_provider.h"

#include "runtime/tool_api.h"
#include "utils/synchronize.h"
#include "utils/work_queue.h"

namespace dsn {

class disk_write_queue : public work_queue<aio_task>
{
public:
    disk_write_queue() : work_queue(2)
    {
        _max_batch_bytes = 1024 * 1024; // 1 MB
    }

private:
    virtual aio_task *unlink_next_workload(void *plength) override;

private:
    uint32_t _max_batch_bytes;
};

class disk_file
{
public:
    explicit disk_file(linux_fd_t fd);
    aio_task *read(aio_task *tsk);
    aio_task *write(aio_task *tsk, void *ctx);

    aio_task *on_read_completed(aio_task *wk, error_code err, size_t size);
    aio_task *on_write_completed(aio_task *wk, void *ctx, error_code err, size_t size);

    linux_fd_t native_handle() const { return _fd; }

private:
    linux_fd_t _fd;
    disk_write_queue _write_queue;
    work_queue<aio_task> _read_queue;
};

class disk_engine : public utils::singleton<disk_engine>
{
public:
    void write(aio_task *aio);
    static aio_provider &provider() { return *instance()._provider.get(); }

private:
    // the object of disk_engine must be created by `singleton::instance`
    disk_engine();
    ~disk_engine() = default;

    void process_write(aio_task *wk, uint64_t sz);
    void complete_io(aio_task *aio, error_code err, uint64_t bytes);

    std::unique_ptr<aio_provider> _provider;

    friend class aio_provider;
    friend class batch_write_io_task;
    friend class utils::singleton<disk_engine>;
};

} // namespace dsn
