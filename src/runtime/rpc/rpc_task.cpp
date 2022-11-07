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

#include "runtime/task/task_engine.h"
#include "runtime/task/task.h"

namespace dsn {

rpc_request_task::rpc_request_task(message_ex *request, rpc_request_handler &&h, service_node *node)
    : task(request->rpc_code(), request->header->client.thread_hash, node),
      _request(request),
      _handler(std::move(h)),
      _enqueue_ts_ns(0)
{
    DCHECK_EQ_MSG(
        TASK_TYPE_RPC_REQUEST,
        spec().type,
        "{} is not a RPC_REQUEST task, please use DEFINE_TASK_CODE_RPC to define the task code",
        spec().name);
    _request->add_ref(); // released in dctor
}

rpc_request_task::~rpc_request_task()
{
    _request->release_ref(); // added in ctor
}

void rpc_request_task::enqueue()
{
    if (spec().rpc_request_dropped_before_execution_when_timeout) {
        _enqueue_ts_ns = dsn_now_ns();
    }
    task::enqueue(node()->computation()->get_pool(spec().pool_code));
}

rpc_response_task::rpc_response_task(message_ex *request,
                                     const rpc_response_handler &cb,
                                     int hash,
                                     service_node *node)
    : rpc_response_task(request, rpc_response_handler(cb), hash, node)
{
}

rpc_response_task::rpc_response_task(message_ex *request,
                                     rpc_response_handler &&cb,
                                     int hash,
                                     service_node *node)
    : task(task_spec::get(request->local_rpc_code)->rpc_paired_code,
           hash == 0 ? request->header->client.thread_hash : hash,
           node),
      _cb(std::move(cb))
{
    _is_null = (_cb == nullptr);

    set_error_code(ERR_IO_PENDING);

    DCHECK_EQ_MSG(TASK_TYPE_RPC_RESPONSE,
                  spec().type,
                  "{} is not of RPC_RESPONSE type, please use DEFINE_TASK_CODE_RPC to define the "
                  "request task code",
                  spec().name);

    _request = request;
    _response = nullptr;

    _caller_pool = get_current_worker() ? get_current_worker()->pool() : nullptr;

    _request->add_ref(); // released in dctor
}

rpc_response_task::~rpc_response_task()
{
    _request->release_ref(); // added in ctor

    if (_response != nullptr)
        _response->release_ref(); // added in enqueue
}

bool rpc_response_task::enqueue(error_code err, message_ex *reply)
{
    set_error_code(err);

    if (_response != nullptr)
        _response->release_ref(); // added in previous enqueue

    _response = reply;

    if (nullptr != reply) {
        reply->add_ref(); // released in dctor
    }

    bool ret = true;
    if (!spec().on_rpc_response_enqueue.execute(this, true)) {
        set_error_code(ERR_NETWORK_FAILURE);
        ret = false;
    }

    rpc_response_task::enqueue();
    return ret;
}

void rpc_response_task::enqueue()
{
    if (_caller_pool)
        task::enqueue(_caller_pool);

    // possible when it is called in non-rDSN threads
    else {
        auto pool = node()->computation()->get_pool(spec().pool_code);
        task::enqueue(pool);
    }
}

} // namespace dsn
