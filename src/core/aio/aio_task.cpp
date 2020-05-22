// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "core/core/task_engine.h"
#include <dsn/tool-api/file_io.h>
#include <dsn/utility/error_code.h>

namespace dsn {

aio_task::aio_task(dsn::task_code code, const aio_handler &cb, int hash, service_node *node)
    : aio_task(code, aio_handler(cb), hash, node)
{
}

aio_task::aio_task(dsn::task_code code, aio_handler &&cb, int hash, service_node *node)
    : task(code, hash, node), _cb(std::move(cb))
{
    _is_null = (_cb == nullptr);

    dassert(TASK_TYPE_AIO == spec().type,
            "%s is not of AIO type, please use DEFINE_TASK_CODE_AIO to define the task code",
            spec().name.c_str());
    set_error_code(ERR_IO_PENDING);

    _aio_ctx = file::prepare_aio_context(this);
}

void aio_task::collapse()
{
    if (!_unmerged_write_buffers.empty()) {
        std::shared_ptr<char> buffer(dsn::utils::make_shared_array<char>(_aio_ctx->buffer_size));
        char *dest = buffer.get();
        for (const dsn_file_buffer_t &b : _unmerged_write_buffers) {
            ::memcpy(dest, b.buffer, b.size);
            dest += b.size;
        }
        dassert(dest - buffer.get() == _aio_ctx->buffer_size,
                "%u VS %u",
                dest - buffer.get(),
                _aio_ctx->buffer_size);
        _aio_ctx->buffer = buffer.get();
        _merged_write_buffer_holder.assign(std::move(buffer), 0, _aio_ctx->buffer_size);
    }
}

void aio_task::enqueue(error_code err, size_t transferred_size)
{
    set_error_code(err);
    _transferred_size = transferred_size;

    spec().on_aio_enqueue.execute(this);

    task::enqueue(node()->computation()->get_pool(spec().pool_code));
}

} // namespace dsn
