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

/*
 * Description:
 *     task and execution model
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/c/api_common.h>
#include <dsn/utility/error_code.h>
#include <dsn/tool-api/threadpool_code.h>
#include <dsn/tool-api/task_code.h>

/*!
@addtogroup task-common
@{
 */
/*! callback prototype for \ref TASK_TYPE_COMPUTE */
typedef void (*dsn_task_handler_t)(void * ///< void* context
                                   );

/*! callback prototype for \ref TASK_TYPE_RPC_REQUEST */
typedef void (*dsn_rpc_request_handler_t)(dsn_message_t, ///< incoming request
                                          void *         ///< handler context registered
                                          );

/*! callback prototype for \ref TASK_TYPE_RPC_RESPONSE */
typedef void (*dsn_rpc_response_handler_t)(
    dsn::error_code, ///< usually, it is ok, or timeout, or busy
    dsn_message_t,   ///< sent rpc request
    dsn_message_t,   ///< incoming rpc response
    void *           ///< context when rpc is called
    );

/*! callback prototype for \ref TASK_TYPE_AIO */
typedef void (*dsn_aio_handler_t)(dsn::error_code, ///< error code for the io operation
                                  size_t,          ///< transferred io size
                                  void *           ///< context when rd/wt is called
                                  );
/*!
 callback prototype for task cancellation (called on task-being-cancelled)

 in rDSN, tasks can be cancelled. For languages such as C++, when there are explicit resource
 release operations (e.g., ::free, release_ref()) in the task handlers, cancellation will
 cause resource leak due to not-executed task handleers. in order to support such scenario,
 rDSN provides dsn_task_cancelled_handler_t which is executed when a task is cancelled. Note
 this callback does not have thread affinity similar to task handlers above (which are
 configured to be executed in certain thread pools or even a fixed thread). Therefore, it is
 developers' resposibility to ensure this cancallation callback only does thread-insensitive
 operations (e.g., release_ref()).
 */
typedef void (*dsn_task_cancelled_handler_t)(
    void * ///< shared with the task handler callbacks, e.g., in \ref dsn_task_handler_t
    );

/*!
apps updates the value at dsn_task_queue_virtual_length_ptr(..) to control
the length of a vitual queue (bound to current code + hash) to
enable customized throttling, see spec of thread pool for more information
*/
extern DSN_API volatile int *dsn_task_queue_virtual_length_ptr(dsn::task_code code,
                                                               int hash DEFAULT(0));

/*@}*/
