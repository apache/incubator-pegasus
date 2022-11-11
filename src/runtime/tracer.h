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

#include "runtime/tool_api.h"

/*!
@defgroup tracer Tracer
@ingroup tools

Tracer toollet

This toollet logs all task operations for the specified tasks,
as configed below.

<PRE>

[core]

toollets = tracer

[task..default]
is_trace = true

; whether to trace when an aio task is called
tracer::on_aio_call = true

; whether to trace when an aio task is enqueued
tracer::on_aio_enqueue = true

; whether to trace when a rpc is made
tracer::on_rpc_call = true

; whether to trace when reply a rpc request
tracer::on_rpc_reply = true

; whether to trace when a rpc request task is enqueued
tracer::on_rpc_request_enqueue = true

; whetehr to trace when a rpc response task is enqueued
tracer::on_rpc_response_enqueue = true

; whether to trace when a task begins
tracer::on_task_begin = true

; whether to trace when a task is cancel post
tracer::on_task_cancel_post = true

; whether to trace when a task is cancelled
tracer::on_task_cancelled = true

; whether to trace when a task ends
tracer::on_task_end = true

; whether to trace when a timer or async task is enqueued
tracer::on_task_enqueue = true

; whether to trace when a task is wait post
tracer::on_task_wait_post = true

; whether to trace when a task is to be wait
tracer::on_task_wait_pre = true

[task.RPC_PING]
is_trace = false

</PRE>
*/
namespace dsn {

class command_deregister;

namespace tools {

class tracer : public toollet
{
public:
    tracer(const char *name);
    void install(service_spec &spec) override;

private:
    std::unique_ptr<command_deregister> _tracer_find_cmd;
};

} // namespace tools
} // namespace dsn
