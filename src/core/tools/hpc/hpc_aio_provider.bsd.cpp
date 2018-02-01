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
 *     AIO using kqueue on BSD
 *
 * Revision history:
 *     2015-09-07, HX Lin(linmajia@live.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#if defined(__APPLE__) || defined(__FreeBSD__)

#include "hpc_aio_provider.h"
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <aio.h>
#include <stdio.h>
#include "mix_all_io_looper.h"

namespace dsn {
namespace tools {

struct posix_disk_aio_context : public disk_aio
{
    struct aiocb cb;
    aio_task *tsk;
    hpc_aio_provider *this_;
    utils::notify_event *evt;
    error_code err;
    uint32_t bytes;
};

#ifndef io_prep_pread
static inline void
io_prep_pread(struct aiocb *iocb, int fd, void *buf, size_t count, long long offset)
{
    memset(iocb, 0, sizeof(*iocb));
    iocb->aio_fildes = fd;
    iocb->aio_lio_opcode = LIO_READ;
    iocb->aio_reqprio = 0;
    iocb->aio_buf = buf;
    iocb->aio_nbytes = count;
    iocb->aio_offset = offset;
}
#endif

#ifndef io_prep_pwrite
static inline void
io_prep_pwrite(struct aiocb *iocb, int fd, void *buf, size_t count, long long offset)
{
    memset(iocb, 0, sizeof(*iocb));
    iocb->aio_fildes = fd;
    iocb->aio_lio_opcode = LIO_WRITE;
    iocb->aio_reqprio = 0;
    iocb->aio_buf = buf;
    iocb->aio_nbytes = count;
    iocb->aio_offset = offset;
}
#endif

hpc_aio_provider::hpc_aio_provider(disk_engine *disk, aio_provider *inner_provider)
    : aio_provider(disk, inner_provider)
{
    _callback = [this](int native_error, uint32_t io_size, uintptr_t lolp_or_events) {
        struct kevent *e;
        struct aiocb *io;
        int bytes;
        int err;

        e = (struct kevent *)lolp_or_events;
        if (e->filter != EVFILT_AIO) {
            derror("Non-aio filter invokes the aio callback! e->filter = %hd", e->filter);
            return;
        }

        io = (struct aiocb *)(e->ident);
        err = aio_error(io);
        if (err == EINPROGRESS) {
            return;
        }

        bytes = aio_return(io);
        if (bytes == -1) {
            err = errno;
        }

        complete_aio(io, bytes, err);
    };

    _looper = nullptr;
}

void hpc_aio_provider::start(io_modifer &ctx)
{
    _looper = get_io_looper(node(), ctx.queue, ctx.mode);
}

hpc_aio_provider::~hpc_aio_provider() {}

dsn_handle_t hpc_aio_provider::open(const char *file_name, int oflag, int pmode)
{
    // No need to bind handle since EVFILT_AIO is registered when aio_* is called.
    return (dsn_handle_t)(uintptr_t)::open(file_name, oflag, pmode);
}

error_code hpc_aio_provider::close(dsn_handle_t fh)
{
    if (::close((int)(uintptr_t)(fh)) == 0) {
        return ERR_OK;
    } else {
        derror("close file failed, err = %s", strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
}

error_code hpc_aio_provider::flush(dsn_handle_t fh)
{
    if (fh == DSN_INVALID_FILE_HANDLE || ::fsync((int)(uintptr_t)(fh)) == 0) {
        return ERR_OK;
    } else {
        derror("flush file failed, err = %s", strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
}

disk_aio *hpc_aio_provider::prepare_aio_context(aio_task *tsk)
{
    auto r = new posix_disk_aio_context;
    bzero((char *)&r->cb, sizeof(r->cb));
    r->tsk = tsk;
    r->evt = nullptr;
    return r;
}

void hpc_aio_provider::aio(aio_task *aio_tsk) { aio_internal(aio_tsk, true); }

error_code hpc_aio_provider::aio_internal(aio_task *aio_tsk,
                                          bool async,
                                          /*out*/ uint32_t *pbytes /*= nullptr*/)
{
    auto aio = (posix_disk_aio_context *)aio_tsk->aio();
    int r = 0;

    aio->this_ = this;

    if (!async) {
        aio->evt = new utils::notify_event();
        aio->err = ERR_OK;
        aio->bytes = 0;
    }

    switch (aio->type) {
    case AIO_Read:
        io_prep_pread(&aio->cb,
                      static_cast<int>((ssize_t)aio->file),
                      aio->buffer,
                      aio->buffer_size,
                      aio->file_offset);
        break;
    case AIO_Write:
        io_prep_pwrite(&aio->cb,
                       static_cast<int>((ssize_t)aio->file),
                       aio->buffer,
                       aio->buffer_size,
                       aio->file_offset);
        break;
    default:
        dassert(false, "unknown aio type %u", static_cast<int>(aio->type));
        break;
    }

    // set up callback
    aio->cb.aio_sigevent.sigev_notify = SIGEV_KEVENT;
    aio->cb.aio_sigevent.sigev_notify_kqueue = (int)(uintptr_t)_looper->native_handle();
    aio->cb.aio_sigevent.sigev_notify_kevent_flags = EV_CLEAR;
    aio->cb.aio_sigevent.sigev_value.sival_ptr = &_callback;

    r = (aio->type == AIO_Read) ? aio_read(&aio->cb) : aio_write(&aio->cb);
    if (r != 0) {
        derror("file op failed, err = %d (%s). On FreeBSD, you may need to load"
               " aio kernel module by running 'sudo kldload aio'.",
               errno,
               strerror(errno));

        if (async) {
            complete_io(aio_tsk, ERR_FILE_OPERATION_FAILED, 0);
        } else {
            delete aio->evt;
            aio->evt = nullptr;
        }
        return ERR_FILE_OPERATION_FAILED;
    } else {
        if (async) {
            return ERR_IO_PENDING;
        } else {
            aio->evt->wait();
            delete aio->evt;
            aio->evt = nullptr;
            if (pbytes != nullptr) {
                *pbytes = aio->bytes;
            }
            return aio->err;
        }
    }
}

void hpc_aio_provider::complete_aio(struct aiocb *io, int bytes, int err)
{
    posix_disk_aio_context *ctx = CONTAINING_RECORD(io, posix_disk_aio_context, cb);
    error_code ec;

    if (err != 0) {
        derror("aio error, err = %s", strerror(err));
        ec = ERR_FILE_OPERATION_FAILED;
    } else {
        dassert(bytes >= 0, "bytes = %d.", bytes);
        ec = bytes > 0 ? ERR_OK : ERR_HANDLE_EOF;
    }

    if (!ctx->evt) {
        aio_task *aio(ctx->tsk);
        ctx->this_->complete_io(aio, ec, bytes);
    } else {
        ctx->err = ec;
        ctx->bytes = bytes;
        ctx->evt->notify();
    }
}
}
} // end namespace dsn::tools
#endif
