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

#include <dsn/tool-api/task.h>
#include <vector>

namespace dsn {

enum aio_type
{
    AIO_Invalid,
    AIO_Read,
    AIO_Write
};

typedef struct
{
    void *buffer;
    int size;
} dsn_file_buffer_t;

class disk_engine;
class aio_context : public ref_counter
{
public:
    // filled by apps
    dsn_handle_t file;
    void *buffer;
    bool support_write_vec; // if the aio provider supports write buffer vector
    std::vector<dsn_file_buffer_t> *write_buffer_vec; // only used if support_write_vec is true
    uint32_t buffer_size;
    uint64_t file_offset;

    // filled by frameworks
    aio_type type;
    disk_engine *engine;
    void *file_object; // TODO(wutao1): make it disk_file*, and distinguish it from `file`

    aio_context()
        : file(nullptr),
          buffer(nullptr),
          support_write_vec(false),
          write_buffer_vec(nullptr),
          buffer_size(0),
          file_offset(0),
          type(AIO_Invalid),
          engine(nullptr),
          file_object(nullptr)
    {
    }
};
typedef dsn::ref_ptr<aio_context> aio_context_ptr;

class aio_task : public task
{
public:
    aio_task(task_code code, const aio_handler &cb, int hash = 0, service_node *node = nullptr);
    aio_task(task_code code, aio_handler &&cb, int hash = 0, service_node *node = nullptr);

    // tell the compiler that we want both the enqueue from base task and ours
    // to prevent the compiler complaining -Werror,-Woverloaded-virtual.
    using task::enqueue;
    void enqueue(error_code err, size_t transferred_size);

    size_t get_transferred_size() const { return _transferred_size; }

    // The ownership of `aio_context` is held by `aio_task`.
    aio_context *get_aio_context() { return _aio_ctx.get(); }

    // merge buffers in _unmerged_write_buffers to a single merged buffer.
    // and store it in _merged_write_buffer_holder.
    void collapse();

    // invoked on aio completed
    virtual void exec() override
    {
        if (nullptr != _cb) {
            _cb(_error, _transferred_size);
        }
    }

    std::vector<dsn_file_buffer_t> _unmerged_write_buffers;
    blob _merged_write_buffer_holder;

protected:
    void clear_non_trivial_on_task_end() override { _cb = nullptr; }

private:
    aio_context_ptr _aio_ctx;
    size_t _transferred_size;
    aio_handler _cb;
};
typedef dsn::ref_ptr<aio_task> aio_task_ptr;

} // namespace dsn
