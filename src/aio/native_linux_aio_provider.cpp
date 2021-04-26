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

#include "native_linux_aio_provider.h"
#include "runtime/service_engine.h"

#include <dsn/tool-api/async_calls.h>
#include <dsn/c/api_utilities.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>

namespace dsn {

native_linux_aio_provider::native_linux_aio_provider(disk_engine *disk) : aio_provider(disk) {}

native_linux_aio_provider::~native_linux_aio_provider() {}

dsn_handle_t native_linux_aio_provider::open(const char *file_name, int flag, int pmode)
{
    dsn_handle_t fh = (dsn_handle_t)(uintptr_t)::open(file_name, flag, pmode);
    if (fh == DSN_INVALID_FILE_HANDLE) {
        derror("create file failed, err = %s", strerror(errno));
    }
    return fh;
}

error_code native_linux_aio_provider::close(dsn_handle_t fh)
{
    if (fh == DSN_INVALID_FILE_HANDLE || ::close((int)(uintptr_t)(fh)) == 0) {
        return ERR_OK;
    } else {
        derror("close file failed, err = %s", strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
}

error_code native_linux_aio_provider::flush(dsn_handle_t fh)
{
    if (fh == DSN_INVALID_FILE_HANDLE || ::fsync((int)(uintptr_t)(fh)) == 0) {
        return ERR_OK;
    } else {
        derror("flush file failed, err = %s", strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
}

error_code native_linux_aio_provider::write(const aio_context &aio_ctx,
                                            /*out*/ uint32_t *processed_bytes)
{
    dsn::error_code resp = ERR_OK;
    uint32_t buffer_offset = 0;
    do {
        // ret is the written data size
        uint32_t ret = pwrite(static_cast<int>((ssize_t)aio_ctx.file),
                              (char *)aio_ctx.buffer + buffer_offset,
                              aio_ctx.buffer_size - buffer_offset,
                              aio_ctx.file_offset + buffer_offset);
        if (dsn_unlikely(ret < 0)) {
            if (errno == EINTR) {
                dwarn_f("write failed with errno={} and will retry it.", strerror(errno));
                continue;
            }
            resp = ERR_FILE_OPERATION_FAILED;
            derror_f("write failed with errno={}, return {}.", strerror(errno), resp);
            return resp;
        }

        // mock the `ret` to reproduce the `write incomplete` case in the first write
        FAIL_POINT_INJECT_VOID_F("aio_pwrite_incomplete", [&]() -> void {
            if (dsn_unlikely(buffer_offset == 0)) {
                --ret;
            }
        });

        buffer_offset += ret;
        if (dsn_unlikely(buffer_offset != aio_ctx.buffer_size)) {
            dwarn_f("write incomplete, request_size={}, total_write_size={}, this_write_size={}, "
                    "and will retry it.",
                    aio_ctx.buffer_size,
                    buffer_offset,
                    ret);
        }
    } while (dsn_unlikely(buffer_offset < aio_ctx.buffer_size));

    *processed_bytes = buffer_offset;
    return resp;
}

error_code native_linux_aio_provider::read(const aio_context &aio_ctx,
                                           /*out*/ uint32_t *processed_bytes)
{
    ssize_t ret = pread(static_cast<int>((ssize_t)aio_ctx.file),
                        aio_ctx.buffer,
                        aio_ctx.buffer_size,
                        aio_ctx.file_offset);
    if (ret < 0) {
        return ERR_FILE_OPERATION_FAILED;
    }
    if (ret == 0) {
        return ERR_HANDLE_EOF;
    }
    *processed_bytes = static_cast<uint32_t>(ret);
    return ERR_OK;
}

void native_linux_aio_provider::submit_aio_task(aio_task *aio_tsk)
{
    // for the tests which use simulator need sync submit for aio
    if (dsn_unlikely(service_engine::instance().is_simulator())) {
        aio_internal(aio_tsk);
        return;
    }

    tasking::enqueue(
        aio_tsk->code(), aio_tsk->tracker(), [=]() { aio_internal(aio_tsk); }, aio_tsk->hash());
}

error_code native_linux_aio_provider::aio_internal(aio_task *aio_tsk)
{
    aio_context *aio_ctx = aio_tsk->get_aio_context();
    error_code err = ERR_UNKNOWN;
    uint32_t processed_bytes = 0;
    switch (aio_ctx->type) {
    case AIO_Read:
        err = read(*aio_ctx, &processed_bytes);
        break;
    case AIO_Write:
        err = write(*aio_ctx, &processed_bytes);
        break;
    default:
        return err;
    }

    complete_io(aio_tsk, err, processed_bytes);
    return err;
}

} // namespace dsn
