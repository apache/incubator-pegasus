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

#include <dsn/tool_api.h>
#include <dsn/utility/synchronize.h>
#include <queue>
#include <stdio.h>       /* for perror() */
#include <sys/syscall.h> /* for __NR_* definitions */
#include <libaio.h>
#include <fcntl.h>    /* O_RDWR */
#include <string.h>   /* memset() */
#include <inttypes.h> /* uint64_t */

namespace dsn {

class native_linux_aio_provider : public aio_provider
{
public:
    native_linux_aio_provider(disk_engine *disk, aio_provider *inner_provider);
    ~native_linux_aio_provider();

    virtual dsn_handle_t open(const char *file_name, int flag, int pmode) override;
    virtual error_code close(dsn_handle_t fh) override;
    virtual error_code flush(dsn_handle_t fh) override;
    virtual void aio(aio_task *aio) override;
    virtual aio_context *prepare_aio_context(aio_task *tsk) override;

    class linux_disk_aio_context : public aio_context
    {
    public:
        struct iocb cb;
        aio_task *tsk;
        native_linux_aio_provider *this_;
        utils::notify_event *evt;
        error_code err;
        uint32_t bytes;

        explicit linux_disk_aio_context(aio_task *tsk_)
            : tsk(tsk_), this_(nullptr), evt(nullptr), err(ERR_UNKNOWN), bytes(0)
        {
        }
    };

protected:
    error_code aio_internal(aio_task *aio, bool async, /*out*/ uint32_t *pbytes = nullptr);
    void complete_aio(struct iocb *io, int bytes, int err);
    void get_event();

private:
    io_context_t _ctx;
    std::atomic<bool> _is_running{false};
    std::thread _worker;
};

} // namespace dsn
