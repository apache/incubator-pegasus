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

#ifdef _WIN32

#include "hpc_aio_provider.h"
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <io.h>
#include <stdio.h>
#include "mix_all_io_looper.h"

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "aio.provider.hpc"

namespace dsn {
namespace tools {

struct windows_disk_aio_context : public disk_aio
{
    OVERLAPPED olp;
    aio_task *tsk;
    utils::notify_event *evt;
    error_code err;
    uint32_t bytes;
};

hpc_aio_provider::hpc_aio_provider(disk_engine *disk, aio_provider *inner_provider)
    : aio_provider(disk, inner_provider)
{
    _looper = nullptr;
    _callback = [this](int native_error, uint32_t io_size, uintptr_t lolp_or_events) {
        windows_disk_aio_context *ctx =
            CONTAINING_RECORD(lolp_or_events, windows_disk_aio_context, olp);
        error_code err =
            native_error == ERROR_SUCCESS
                ? ERR_OK
                : (native_error == ERROR_HANDLE_EOF ? ERR_HANDLE_EOF : ERR_FILE_OPERATION_FAILED);
        if (!ctx->evt) {
            aio_task *aio(ctx->tsk);
            this->complete_io(aio, err, io_size);
        } else {
            ctx->err = err;
            ctx->bytes = io_size;
            ctx->evt->notify();
        }
    };
}

void hpc_aio_provider::start(io_modifer &ctx)
{
    _looper = get_io_looper(node(), ctx.queue, ctx.mode);
}

hpc_aio_provider::~hpc_aio_provider() {}

dsn_handle_t hpc_aio_provider::open(const char *file_name, int oflag, int pmode)
{
    DWORD dwDesiredAccess = 0;
    DWORD dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD dwCreationDisposition = 0;
    DWORD dwFlagsAndAttributes = FILE_FLAG_OVERLAPPED;

    SECURITY_ATTRIBUTES SecurityAttributes;

    SecurityAttributes.nLength = sizeof(SecurityAttributes);
    SecurityAttributes.lpSecurityDescriptor = NULL;

    if (oflag & _O_NOINHERIT) {
        SecurityAttributes.bInheritHandle = FALSE;
    } else {
        SecurityAttributes.bInheritHandle = TRUE;
    }

    /*
    * decode the access flags
    */
    switch (oflag & (_O_RDONLY | _O_WRONLY | _O_RDWR)) {

    case _O_RDONLY: /* read access */
        dwDesiredAccess = GENERIC_READ;
        break;
    case _O_WRONLY: /* write access */
        /* giving it read access as well
        * because in append (a, not a+), we need
        * to read the BOM to determine the encoding
        * (ie. ANSI, UTF8, UTF16)
        */
        if ((oflag & _O_APPEND) && (oflag & (_O_WTEXT | _O_U16TEXT | _O_U8TEXT)) != 0) {
            dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
        } else {
            dwDesiredAccess = GENERIC_WRITE;
        }
        break;
    case _O_RDWR: /* read and write access */
        dwDesiredAccess = GENERIC_READ | GENERIC_WRITE;
        break;
    default:            /* error, bad oflag */
        _doserrno = 0L; /* not an OS error */
        derror("Invalid open flag");
    }

    /*
    * decode open/create method flags
    */
    switch (oflag & (_O_CREAT | _O_EXCL | _O_TRUNC)) {
    case 0:
    case _O_EXCL: // ignore EXCL w/o CREAT
        dwCreationDisposition = OPEN_EXISTING;
        break;

    case _O_CREAT:
        dwCreationDisposition = OPEN_ALWAYS;
        break;

    case _O_CREAT | _O_EXCL:
    case _O_CREAT | _O_TRUNC | _O_EXCL:
        dwCreationDisposition = CREATE_NEW;
        break;

    case _O_TRUNC:
    case _O_TRUNC | _O_EXCL: // ignore EXCL w/o CREAT
        dwCreationDisposition = TRUNCATE_EXISTING;
        break;

    case _O_CREAT | _O_TRUNC:
        dwCreationDisposition = CREATE_ALWAYS;
        break;

    default:
        // this can't happen ... all cases are covered
        _doserrno = 0L;
        derror("Invalid open flag");
    }

    /*
    * try to open/create the file
    */
    HANDLE fileHandle = ::CreateFileA(file_name,
                                      dwDesiredAccess,
                                      dwShareMode,
                                      &SecurityAttributes,
                                      dwCreationDisposition,
                                      dwFlagsAndAttributes,
                                      0);

    if (fileHandle != INVALID_HANDLE_VALUE && fileHandle != nullptr) {
        auto err = _looper->bind_io_handle((dsn_handle_t)fileHandle, &_callback);
        if (err != ERR_OK) {
            dassert(false,
                    "cannot associate file handle %s to io completion port, err = 0x%x",
                    file_name,
                    ::GetLastError());
            return 0;
        } else {
            return (dsn_handle_t)(fileHandle);
        }
    } else {
        derror("cannot create file %s, err = 0x%x", file_name, ::GetLastError());
        return 0;
    }
}

error_code hpc_aio_provider::close(dsn_handle_t fh)
{
    if (::CloseHandle((HANDLE)(fh)))
        return ERR_OK;
    else {
        derror("close file failed, err = 0x%x", ::GetLastError());
        return ERR_FILE_OPERATION_FAILED;
    }
}

error_code hpc_aio_provider::flush(dsn_handle_t fh)
{
    if (fh == DSN_INVALID_FILE_HANDLE || ::FlushFileBuffers((HANDLE)(fh))) {
        return ERR_OK;
    } else {
        derror("close file failed, err = 0x%x", ::GetLastError());
        return ERR_FILE_OPERATION_FAILED;
    }
}

disk_aio *hpc_aio_provider::prepare_aio_context(aio_task *tsk)
{
    auto r = new windows_disk_aio_context;
    ZeroMemory(&r->olp, sizeof(r->olp));
    r->tsk = tsk;
    r->evt = nullptr;
    return r;
}

void hpc_aio_provider::aio(aio_task *aio_tsk) { aio_internal(aio_tsk, true); }

error_code hpc_aio_provider::aio_internal(aio_task *aio_tsk,
                                          bool async,
                                          /*out*/ uint32_t *pbytes /*= nullptr*/)
{
    auto aio = (windows_disk_aio_context *)aio_tsk->aio();
    BOOL r = FALSE;

    aio->olp.Offset = (uint32_t)aio->file_offset;
    aio->olp.OffsetHigh = (uint32_t)(aio->file_offset >> 32);

    if (!async) {
        aio->evt = new utils::notify_event();
        aio->err = ERR_OK;
        aio->bytes = 0;
    }

    switch (aio->type) {
    case AIO_Read:
        r = ::ReadFile((HANDLE)aio->file, aio->buffer, aio->buffer_size, NULL, &aio->olp);
        break;
    case AIO_Write:
        r = ::WriteFile((HANDLE)aio->file, aio->buffer, aio->buffer_size, NULL, &aio->olp);
        break;
    default:
        dassert(false, "unknown aio type %u", static_cast<int>(aio->type));
        break;
    }

    if (!r) {
        int native_error = ::GetLastError();

        if (native_error != ERROR_IO_PENDING) {
            derror("file operation failed, err = %u", native_error);

            error_code err = native_error == ERROR_SUCCESS
                                 ? ERR_OK
                                 : (native_error == ERROR_HANDLE_EOF ? ERR_HANDLE_EOF
                                                                     : ERR_FILE_OPERATION_FAILED);

            if (async) {
                complete_io(aio_tsk, err, 0);
            } else {
                delete aio->evt;
                aio->evt = nullptr;
            }

            return err;
        }
    }

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
} // end namespace dsn::tools
#endif
