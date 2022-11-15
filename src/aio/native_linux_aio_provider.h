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

namespace dsn {

class native_linux_aio_provider : public aio_provider
{
public:
    explicit native_linux_aio_provider(disk_engine *disk);
    ~native_linux_aio_provider() override;

    linux_fd_t open(const char *file_name, int flag, int pmode) override;
    error_code close(linux_fd_t fd) override;
    error_code flush(linux_fd_t fd) override;
    error_code write(const aio_context &aio_ctx, /*out*/ uint64_t *processed_bytes) override;
    error_code read(const aio_context &aio_ctx, /*out*/ uint64_t *processed_bytes) override;

    void submit_aio_task(aio_task *aio) override;
    aio_context *prepare_aio_context(aio_task *tsk) override { return new aio_context; }

protected:
    error_code aio_internal(aio_task *aio);
};

} // namespace dsn
