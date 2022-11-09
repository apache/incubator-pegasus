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

// rDSN uses event-driven programming model, and
// this file defines the task(i.e., event) abstraction and related

#pragma once

#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"

/*!
@addtogroup task-common
@{
 */

namespace dsn {
class message_ex;

typedef std::function<void()> task_handler;

/// A callback to handle rpc requests.
///
/// Parameters:
///  - dsn::message_ex*: the received rpc request
typedef std::function<void(dsn::message_ex *)> rpc_request_handler;

/// A callback to handle rpc responses.
///
/// Parameters:
///  - error_code
///  - message_ex: the sent rpc request
///  - message_ex: the received rpc response
typedef std::function<void(dsn::error_code, dsn::message_ex *, dsn::message_ex *)>
    rpc_response_handler;

/// Parameters:
///  - error_code
///  - size_t: the read or written size of bytes from file.
typedef std::function<void(dsn::error_code, size_t)> aio_handler;

class task;
class raw_task;
class rpc_request_task;
class rpc_response_task;
class aio_task;
}
/*!
apps updates the value at dsn_task_queue_virtual_length_ptr(..) to control
the length of a vitual queue (bound to current code + hash) to
enable customized throttling, see spec of thread pool for more information
*/
extern volatile int *dsn_task_queue_virtual_length_ptr(dsn::task_code code, int hash = 0);

/*@}*/
