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

#include <fcntl.h>

#include "aio/aio_task.h"

namespace dsn {

// forward declaration
class disk_file;

namespace file {

/// open file
///
/// \param file_name filename of the file.
/// \param flag      flags such as O_RDONLY | O_BINARY used by ::open
/// \param pmode     permission mode used by ::open
///
/// \return file handle
///
extern disk_file *open(const char *file_name, int flag, int pmode);

/// close the file handle
extern error_code close(disk_file *file);

/// flush the buffer of the given file
extern error_code flush(disk_file *file);

inline aio_task_ptr
create_aio_task(task_code code, task_tracker *tracker, aio_handler &&callback, int hash = 0)
{
    aio_task_ptr t(new aio_task(code, std::move(callback), hash));
    t->set_tracker((task_tracker *)tracker);
    t->spec().on_task_create.execute(task::get_current_task(), t);
    return t;
}

extern aio_task_ptr read(disk_file *file,
                         char *buffer,
                         int count,
                         uint64_t offset,
                         task_code callback_code,
                         task_tracker *tracker,
                         aio_handler &&callback,
                         int hash = 0);

extern aio_task_ptr write(disk_file *file,
                          const char *buffer,
                          int count,
                          uint64_t offset,
                          task_code callback_code,
                          task_tracker *tracker,
                          aio_handler &&callback,
                          int hash = 0);

extern aio_task_ptr write_vector(disk_file *file,
                                 const dsn_file_buffer_t *buffers,
                                 int buffer_count,
                                 uint64_t offset,
                                 task_code callback_code,
                                 task_tracker *tracker,
                                 aio_handler &&callback,
                                 int hash = 0);

extern aio_context_ptr prepare_aio_context(aio_task *tsk);

} // namespace file
} // namespace dsn
