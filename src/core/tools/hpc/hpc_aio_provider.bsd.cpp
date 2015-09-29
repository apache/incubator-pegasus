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

# if defined(__APPLE__) || defined(__FreeBSD__)

# include "hpc_aio_provider.h"
# include <fcntl.h>
# include <sys/types.h>
# include <sys/stat.h>
# include <aio.h>
# include <stdio.h>
# include "mix_all_io_looper.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "aio.provider.hpc"

namespace dsn { namespace tools {

struct posix_disk_aio_context : public disk_aio
{
    struct aiocb cb;
    aio_task* tsk;
    hpc_aio_provider* this_;
    utils::notify_event* evt;
    error_code err;
    uint32_t bytes;
};

hpc_aio_provider::hpc_aio_provider(disk_engine* disk, aio_provider* inner_provider)
    : aio_provider(disk, inner_provider)
{
    _callback = [this](
        int native_error,
        uint32_t io_size,
        uintptr_t lolp_or_events
        )
    {
        auto e = (struct kevent*)lolp_or_events;
        auto io = (struct aiocb*)(e->ident);
        complete_aio(io);
    };

    _looper = nullptr;
}

void hpc_aio_provider::start(io_modifer& ctx)
{
    _looper = get_io_looper(node(), ctx.queue, ctx.mode);
}

hpc_aio_provider::~hpc_aio_provider()
{
}

dsn_handle_t hpc_aio_provider::open(const char* file_name, int oflag, int pmode)
{
    // No need to bind handle since EVFILT_AIO is registered when aio_* is called.
    return  (dsn_handle_t)(uintptr_t)::open(file_name, oflag, pmode);
}

error_code hpc_aio_provider::close(dsn_handle_t hFile)
{
    auto error = (::close((int)(uintptr_t)(hFile)) == 0) ? ERR_OK : ERR_FILE_OPERATION_FAILED;
    return error;
}

disk_aio* hpc_aio_provider::prepare_aio_context(aio_task* tsk)
{
    auto r = new posix_disk_aio_context;
    bzero((char*)&r->cb, sizeof(r->cb));
    r->tsk = tsk;
    r->evt = nullptr;
    return r;
}

void hpc_aio_provider::aio(aio_task* aio_tsk)
{
    auto err = aio_internal(aio_tsk, true);
    err.end_tracking();
}

error_code hpc_aio_provider::aio_internal(aio_task* aio_tsk, bool async, /*out*/ uint32_t* pbytes /*= nullptr*/)
{
    auto aio = (posix_disk_aio_context *)aio_tsk->aio();
    int r;

    aio->this_ = this;
    aio->cb.aio_fildes = static_cast<int>((ssize_t)aio->file);
    aio->cb.aio_buf = aio->buffer;
    aio->cb.aio_nbytes = aio->buffer_size;
    aio->cb.aio_offset = aio->file_offset;

    // set up callback
    aio->cb.aio_sigevent.sigev_notify = SIGEV_KEVENT;
    aio->cb.aio_sigevent.sigev_notify_kqueue = (int)(uintptr_t)_looper->native_handle();
    aio->cb.aio_sigevent.sigev_notify_kevent_flags = EV_CLEAR;
    aio->cb.aio_sigevent.sigev_value.sival_ptr = &_callback;

    if (!async)
    {
        aio->evt = new utils::notify_event();
        aio->err = ERR_OK;
        aio->bytes = 0;
    }

    switch (aio->type)
    {
    case AIO_Read:
        r = aio_read(&aio->cb);
        break;
    case AIO_Write:
        r = aio_write(&aio->cb);
        break;
    default:
        dassert(false, "unknown aio type %u", static_cast<int>(aio->type));
        break;
    }

    if (r != 0)
    {
        derror("file op failed, err = %d (%s). On FreeBSD, you may need to load"
            " aio kernel module by running 'sudo kldload aio'.", errno, strerror(errno));

        if (async)
        {
            complete_io(aio_tsk, ERR_FILE_OPERATION_FAILED, 0);
        }
        else
        {
            delete aio->evt;
            aio->evt = nullptr;
        }
        return ERR_FILE_OPERATION_FAILED;
    }
    else
    {
        if (async)
        {
            return ERR_IO_PENDING;
        }
        else
        {
            aio->evt->wait();
            delete aio->evt;
            aio->evt = nullptr;
            if (pbytes != nullptr)
            {
                *pbytes = aio->bytes;
            }
            return aio->err;
        }
    }
}

void hpc_aio_provider::complete_aio(struct aiocb* io)
{
    posix_disk_aio_context* ctx = CONTAINING_RECORD(io, posix_disk_aio_context, cb);

    int err = aio_error(&ctx->cb);
    if (err != EINPROGRESS)
    {
        size_t bytes = aio_return(&ctx->cb); // from e.g., read or write
        error_code ec;
        if (err != 0)
        {
            derror("aio error, err = %s", strerror(err));
            ec = ERR_FILE_OPERATION_FAILED;
        }
        else
        {
            ec = bytes > 0 ? ERR_OK : ERR_HANDLE_EOF;
        }
        
        if (!ctx->evt)
        {
            aio_task* aio(ctx->tsk);
            ctx->this_->complete_io(aio, ec, bytes);
        }
        else
        {
            ctx->err = ec;
            ctx->bytes = bytes;
            ctx->evt->notify();
        }
    }
}

}} // end namespace dsn::tools
#endif
