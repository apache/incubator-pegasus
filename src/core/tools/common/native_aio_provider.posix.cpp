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

#ifndef _WIN32

#include "native_aio_provider.posix.h"

#include <aio.h>
#include <fcntl.h>
#include <cstdlib>

namespace dsn {
namespace tools {

native_posix_aio_provider::native_posix_aio_provider(disk_engine *disk,
                                                     aio_provider *inner_provider)
    : aio_provider(disk, inner_provider)
{
}

native_posix_aio_provider::~native_posix_aio_provider() {}

dsn_handle_t native_posix_aio_provider::open(const char *file_name, int flag, int pmode)
{
    dsn_handle_t fh = (dsn_handle_t)(uintptr_t)::open(file_name, flag, pmode);
    if (fh == DSN_INVALID_FILE_HANDLE) {
        derror("create file failed, err = %s", strerror(errno));
    }
    return fh;
}

error_code native_posix_aio_provider::close(dsn_handle_t fh)
{
    if (fh == DSN_INVALID_FILE_HANDLE || ::close((int)(uintptr_t)(fh)) == 0) {
        return ERR_OK;
    } else {
        derror("close file failed, err = %s", strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
}

error_code native_posix_aio_provider::flush(dsn_handle_t fh)
{
    if (fh == DSN_INVALID_FILE_HANDLE || ::fsync((int)(uintptr_t)(fh)) == 0) {
        return ERR_OK;
    } else {
        derror("flush file failed, err = %s", strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
}

class posix_disk_aio_context : public disk_aio
{
public:
    struct aiocb cb;
    aio_task *tsk;
    native_posix_aio_provider *this_;
    utils::notify_event *evt;
    error_code err;
    uint32_t bytes;

    explicit posix_disk_aio_context(aio_task *tsk_)
        : disk_aio(), tsk(tsk_), this_(nullptr), evt(nullptr), err(ERR_UNKNOWN), bytes(0)
    {
        ::bzero((char *)&cb, sizeof(cb));
    }
};

disk_aio *native_posix_aio_provider::prepare_aio_context(aio_task *tsk)
{
    return new posix_disk_aio_context(tsk);
}

void native_posix_aio_provider::aio(aio_task *aio_tsk) { aio_internal(aio_tsk, true); }

void aio_completed(sigval sigval)
{
    auto ctx = (posix_disk_aio_context *)sigval.sival_ptr;

    if (dsn::tls_dsn.magic != 0xdeadbeef)
        task::set_tls_dsn_context(ctx->tsk->node(), nullptr);

    int err = aio_error(&ctx->cb);
    if (err != EINPROGRESS) {
        size_t bytes = aio_return(&ctx->cb); // from e.g., read or write
        error_code ec;
        if (err != 0) {
            derror("aio error, err = %s", strerror(err));
            ec = ERR_FILE_OPERATION_FAILED;
        } else {
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

error_code native_posix_aio_provider::aio_internal(aio_task *aio_tsk,
                                                   bool async,
                                                   /*out*/ uint32_t *pbytes /*= nullptr*/)
{
    auto aio = (posix_disk_aio_context *)aio_tsk->aio();
    int r = 0;

    aio->this_ = this;
    memset(&aio->cb, 0, sizeof(aio->cb));
    aio->cb.aio_reqprio = 0;
    aio->cb.aio_lio_opcode = (aio->type == AIO_Read ? LIO_READ : LIO_WRITE);
    aio->cb.aio_fildes = static_cast<int>((ssize_t)aio->file);
    aio->cb.aio_buf = aio->buffer;
    aio->cb.aio_nbytes = aio->buffer_size;
    aio->cb.aio_offset = aio->file_offset;

    // set up callback
    aio->cb.aio_sigevent.sigev_notify = SIGEV_THREAD;
    aio->cb.aio_sigevent.sigev_notify_function = aio_completed;
    aio->cb.aio_sigevent.sigev_notify_attributes = nullptr;
    aio->cb.aio_sigevent.sigev_value.sival_ptr = aio;

    if (!async) {
        aio->evt = new utils::notify_event();
        aio->err = ERR_OK;
        aio->bytes = 0;
    }

    switch (aio->type) {
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
}
} // end namespace dsn::tools

#endif
