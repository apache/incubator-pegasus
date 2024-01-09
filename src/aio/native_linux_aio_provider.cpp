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

#include "aio/aio_provider.h"
#include "aio/disk_engine.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "runtime/service_engine.h"
#include "task/async_calls.h"
#include "utils/env.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"
#include "utils/ports.h"

namespace dsn {

native_linux_aio_provider::native_linux_aio_provider(disk_engine *disk) : aio_provider(disk) {}

native_linux_aio_provider::~native_linux_aio_provider() {}

std::unique_ptr<rocksdb::RandomAccessFile>
native_linux_aio_provider::open_read_file(const std::string &fname)
{
    std::unique_ptr<rocksdb::RandomAccessFile> rfile;
    auto s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)
                 ->NewRandomAccessFile(fname, &rfile, rocksdb::EnvOptions());
    if (!s.ok()) {
        LOG_ERROR("open read file '{}' failed, err = {}", fname, s.ToString());
    }
    return rfile;
}

std::unique_ptr<rocksdb::RandomRWFile>
native_linux_aio_provider::open_write_file(const std::string &fname)
{
    // rocksdb::NewRandomRWFile() doesn't act as the docs described, it will not create the
    // file if it not exists, and an error Status will be returned, so we try to create the
    // file by ReopenWritableFile() if it not exist.
    auto s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)->FileExists(fname);
    if (!s.ok() && !s.IsNotFound()) {
        LOG_ERROR("failed to check whether the file '{}' exist, err = {}", fname, s.ToString());
        return nullptr;
    }

    if (s.IsNotFound()) {
        std::unique_ptr<rocksdb::WritableFile> cfile;
        s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)
                ->ReopenWritableFile(fname, &cfile, rocksdb::EnvOptions());
        if (!s.ok()) {
            LOG_ERROR("failed to create file '{}', err = {}", fname, s.ToString());
            return nullptr;
        }
    }

    // Open the file for write as RandomRWFile, to support un-sequential write.
    std::unique_ptr<rocksdb::RandomRWFile> wfile;
    s = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive)
            ->NewRandomRWFile(fname, &wfile, rocksdb::EnvOptions());
    if (!s.ok()) {
        LOG_ERROR("open write file '{}' failed, err = {}", fname, s.ToString());
    }
    return wfile;
}

error_code native_linux_aio_provider::close(rocksdb::RandomRWFile *wf)
{
    auto s = wf->Close();
    if (!s.ok()) {
        LOG_ERROR("close file failed, err = {}", s.ToString());
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

error_code native_linux_aio_provider::flush(rocksdb::RandomRWFile *wf)
{
    auto s = wf->Fsync();
    if (!s.ok()) {
        LOG_ERROR("flush file failed, err = {}", s.ToString());
        return ERR_FILE_OPERATION_FAILED;
    }

    return ERR_OK;
}

error_code native_linux_aio_provider::write(const aio_context &aio_ctx,
                                            /*out*/ uint64_t *processed_bytes)
{
    rocksdb::Slice data((const char *)(aio_ctx.buffer), aio_ctx.buffer_size);
    auto s = aio_ctx.dfile->wfile()->Write(aio_ctx.file_offset, data);
    if (!s.ok()) {
        LOG_ERROR("write file failed, err = {}", s.ToString());
        return ERR_FILE_OPERATION_FAILED;
    }

    *processed_bytes = aio_ctx.buffer_size;
    return ERR_OK;
}

error_code native_linux_aio_provider::read(const aio_context &aio_ctx,
                                           /*out*/ uint64_t *processed_bytes)
{
    rocksdb::Slice result;
    auto s = aio_ctx.dfile->rfile()->Read(
        aio_ctx.file_offset, aio_ctx.buffer_size, &result, (char *)(aio_ctx.buffer));
    if (!s.ok()) {
        LOG_ERROR("read file failed, err = {}", s.ToString());
        return ERR_FILE_OPERATION_FAILED;
    }

    if (result.empty()) {
        return ERR_HANDLE_EOF;
    }
    *processed_bytes = result.size();
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
