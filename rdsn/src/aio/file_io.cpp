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

#include "disk_engine.h"
#include <dsn/tool-api/file_io.h>

namespace dsn {
namespace file {

/*extern*/ disk_file *open(const char *file_name, int flag, int pmode)
{
    dsn_handle_t nh = disk_engine::provider().open(file_name, flag, pmode);
    if (nh != DSN_INVALID_FILE_HANDLE) {
        return new disk_file(nh);
    } else {
        return nullptr;
    }
}

/*extern*/ error_code close(disk_file *file)
{
    error_code result = ERR_INVALID_HANDLE;
    if (file != nullptr) {
        result = disk_engine::provider().close(file->native_handle());
        delete file;
        file = nullptr;
    }
    return result;
}

/*extern*/ error_code flush(disk_file *file)
{
    if (nullptr != file) {
        return disk_engine::provider().flush(file->native_handle());
    } else {
        return ERR_INVALID_HANDLE;
    }
}

/*extern*/ aio_task_ptr read(disk_file *file,
                             char *buffer,
                             int count,
                             uint64_t offset,
                             task_code callback_code,
                             task_tracker *tracker,
                             aio_handler &&callback,
                             int hash /*= 0*/)
{
    auto cb = create_aio_task(callback_code, tracker, std::move(callback), hash);
    cb->get_aio_context()->buffer = buffer;
    cb->get_aio_context()->buffer_size = count;
    cb->get_aio_context()->file_object = file;
    cb->get_aio_context()->file = file->native_handle();
    cb->get_aio_context()->file_offset = offset;
    cb->get_aio_context()->type = AIO_Read;
    cb->get_aio_context()->engine = &disk_engine::instance();

    if (!cb->spec().on_aio_call.execute(task::get_current_task(), cb, true)) {
        cb->enqueue(ERR_FILE_OPERATION_FAILED, 0);
        return cb;
    }
    auto wk = file->read(cb);
    if (wk) {
        disk_engine::provider().submit_aio_task(wk);
    }
    return cb;
}

/*extern*/ aio_task_ptr write(disk_file *file,
                              const char *buffer,
                              int count,
                              uint64_t offset,
                              task_code callback_code,
                              task_tracker *tracker,
                              aio_handler &&callback,
                              int hash /*= 0*/)
{
    auto cb = create_aio_task(callback_code, tracker, std::move(callback), hash);
    cb->get_aio_context()->buffer = (char *)buffer;
    cb->get_aio_context()->buffer_size = count;
    cb->get_aio_context()->file = file;
    cb->get_aio_context()->file_offset = offset;
    cb->get_aio_context()->type = AIO_Write;

    disk_engine::instance().write(cb);
    return cb;
}

/*extern*/ aio_task_ptr write_vector(disk_file *file,
                                     const dsn_file_buffer_t *buffers,
                                     int buffer_count,
                                     uint64_t offset,
                                     task_code callback_code,
                                     task_tracker *tracker,
                                     aio_handler &&callback,
                                     int hash /*= 0*/)
{
    auto cb = create_aio_task(callback_code, tracker, std::move(callback), hash);
    cb->get_aio_context()->file = file;
    cb->get_aio_context()->file_offset = offset;
    cb->get_aio_context()->type = AIO_Write;
    for (int i = 0; i < buffer_count; i++) {
        if (buffers[i].size > 0) {
            cb->_unmerged_write_buffers.push_back(buffers[i]);
            cb->get_aio_context()->buffer_size += buffers[i].size;
        }
    }

    disk_engine::instance().write(cb);
    return cb;
}

/*extern*/ aio_context_ptr prepare_aio_context(aio_task *tsk)
{
    return disk_engine::provider().prepare_aio_context(tsk);
}
} // namespace file
} // namespace dsn
