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

#include <fcntl.h>

#include "aio/disk_engine.h"
#include "runtime/service_engine.h"
#include "runtime/task/async_calls.h"
#include "utils/api_utilities.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"
#include "utils/safe_strerror_posix.h"

namespace dsn {

native_linux_aio_provider::native_linux_aio_provider(disk_engine *disk) : aio_provider(disk) {}

native_linux_aio_provider::~native_linux_aio_provider() {}

linux_fd_t native_linux_aio_provider::open(const char *file_name, int flag, int pmode)
{
    auto fd = ::open(file_name, flag, pmode);
    if (fd == DSN_INVALID_FILE_HANDLE) {
        LOG_ERROR_F("create file failed, err = {}", utils::safe_strerror(errno));
    }
    return linux_fd_t(fd);
}

error_code native_linux_aio_provider::close(linux_fd_t fd)
{
    if (fd.is_invalid() || ::close(fd.fd) == 0) {
        return ERR_OK;
    }

    LOG_ERROR_F("close file failed, err = {}", utils::safe_strerror(errno));
    return ERR_FILE_OPERATION_FAILED;
}

error_code native_linux_aio_provider::flush(linux_fd_t fd)
{
    if (fd.is_invalid() || ::fsync(fd.fd) == 0) {
        return ERR_OK;
    }

    LOG_ERROR_F("flush file failed, err = {}", utils::safe_strerror(errno));
    return ERR_FILE_OPERATION_FAILED;
}

error_code native_linux_aio_provider::write(const aio_context &aio_ctx,
                                            /*out*/ uint64_t *processed_bytes)
{
    dsn::error_code resp = ERR_OK;
    uint64_t buffer_offset = 0;
    do {
        // ret is the written data size
        auto ret = ::pwrite(aio_ctx.dfile->native_handle().fd,
                            (char *)aio_ctx.buffer + buffer_offset,
                            aio_ctx.buffer_size - buffer_offset,
                            aio_ctx.file_offset + buffer_offset);
        if (dsn_unlikely(ret < 0)) {
            if (errno == EINTR) {
                LOG_WARNING_F("write failed with errno={} and will retry it.",
                              utils::safe_strerror(errno));
                continue;
            }
            resp = ERR_FILE_OPERATION_FAILED;
            LOG_ERROR_F(
                "write failed with errno={}, return {}.", utils::safe_strerror(errno), resp);
            return resp;
        }

        // mock the `ret` to reproduce the `write incomplete` case in the first write
        FAIL_POINT_INJECT_NOT_RETURN_F("aio_pwrite_incomplete", [&](string_view s) -> void {
            if (dsn_unlikely(buffer_offset == 0)) {
                --ret;
            }
        });

        buffer_offset += ret;
        if (dsn_unlikely(buffer_offset != aio_ctx.buffer_size)) {
            LOG_WARNING_F(
                "write incomplete, request_size={}, total_write_size={}, this_write_size={}, "
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
                                           /*out*/ uint64_t *processed_bytes)
{
    auto ret = ::pread(aio_ctx.dfile->native_handle().fd,
                       aio_ctx.buffer,
                       aio_ctx.buffer_size,
                       aio_ctx.file_offset);
    if (dsn_unlikely(ret < 0)) {
        LOG_WARNING_F("write failed with errno={} and will retry it.", utils::safe_strerror(errno));
        return ERR_FILE_OPERATION_FAILED;
    }
    if (ret == 0) {
        return ERR_HANDLE_EOF;
    }
    *processed_bytes = static_cast<uint64_t>(ret);
    return ERR_OK;
}

void native_linux_aio_provider::submit_aio_task(aio_task *aio_tsk)
{
    // for the tests which use simulator need sync submit for aio
    if (dsn_unlikely(service_engine::instance().is_simulator())) {
        aio_internal(aio_tsk);
        return;
    }

    ADD_POINT(aio_tsk->_tracer);
    tasking::enqueue(
        aio_tsk->code(), aio_tsk->tracker(), [=]() { aio_internal(aio_tsk); }, aio_tsk->hash());
}

error_code native_linux_aio_provider::aio_internal(aio_task *aio_tsk)
{
    ADD_POINT(aio_tsk->_tracer);
    aio_context *aio_ctx = aio_tsk->get_aio_context();
    error_code err = ERR_UNKNOWN;
    uint64_t processed_bytes = 0;
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

    ADD_CUSTOM_POINT(aio_tsk->_tracer, "completed");

    complete_io(aio_tsk, err, processed_bytes);
    return err;
}

} // namespace dsn
