// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <string.h>
#include <functional>
#include <list>
#include <memory>
#include <utility>
#include <vector>

#include "aio/aio_task.h"
#include "aio/file_io.h"
#include "runtime/api_task.h"
#include "runtime/service_engine.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_engine.h"
#include "runtime/task/task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/latency_tracer.h"
#include "utils/threadpool_code.h"
#include "utils/utils.h"

namespace dsn {

aio_task::aio_task(dsn::task_code code, const aio_handler &cb, int hash, service_node *node)
    : aio_task(code, aio_handler(cb), hash, node)
{
}

aio_task::aio_task(dsn::task_code code, aio_handler &&cb, int hash, service_node *node)
    : task(code, hash, node), _cb(std::move(cb))
{
    _is_null = (_cb == nullptr);

    CHECK_EQ_MSG(TASK_TYPE_AIO,
                 spec().type,
                 "{} is not of AIO type, please use DEFINE_TASK_CODE_AIO to define the task code",
                 spec().name);
    set_error_code(ERR_IO_PENDING);

    _aio_ctx = file::prepare_aio_context(this);

    _tracer = std::make_shared<dsn::utils::latency_tracer>(true, "aio_task", 0, code);
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
        CHECK_EQ(dest - buffer.get(), _aio_ctx->buffer_size);
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
