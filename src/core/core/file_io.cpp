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

#include <dsn/tool-api/file_io.h>

#include "disk_engine.h"

namespace dsn {
namespace file {

/*extern*/ disk_file *open(const char *file_name, int flag, int pmode)
{
    return task::get_current_disk()->open(file_name, flag, pmode);
}

/*extern*/ error_code close(disk_file *file) { return task::get_current_disk()->close(file); }

/*extern*/ error_code flush(disk_file *file) { return task::get_current_disk()->flush(file); }

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
    cb->aio()->buffer = buffer;
    cb->aio()->buffer_size = count;
    cb->aio()->file = file;
    cb->aio()->file_offset = offset;
    cb->aio()->type = AIO_Read;

    task::get_current_disk()->read(cb);
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
    cb->aio()->buffer = (char *)buffer;
    cb->aio()->buffer_size = count;
    cb->aio()->file = file;
    cb->aio()->file_offset = offset;
    cb->aio()->type = AIO_Write;

    task::get_current_disk()->write(cb);
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
    cb->aio()->file = file;
    cb->aio()->file_offset = offset;
    cb->aio()->type = AIO_Write;
    for (int i = 0; i < buffer_count; i++) {
        if (buffers[i].size > 0) {
            cb->_unmerged_write_buffers.push_back(buffers[i]);
            cb->aio()->buffer_size += buffers[i].size;
        }
    }

    task::get_current_disk()->write(cb);
    return cb;
}

} // namespace file
} // namespace dsn
