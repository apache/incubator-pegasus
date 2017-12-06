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
#include <dsn/tool-api/threadpool_code.h>

/*!
@addtogroup task-common
@{
 */

/*! task/event type definition */
typedef enum dsn_task_type_t {
    TASK_TYPE_RPC_REQUEST,  ///< task handling rpc request
    TASK_TYPE_RPC_RESPONSE, ///< task handling rpc response or timeout
    TASK_TYPE_COMPUTE,      ///< async calls or timers
    TASK_TYPE_AIO,          ///< callback for file read and write
    TASK_TYPE_CONTINUATION, ///< above tasks are seperated into several continuation
                            ///< tasks by thread-synchronization operations.
                            ///< so that each "task" is non-blocking
    TASK_TYPE_COUNT,
    TASK_TYPE_INVALID
} dsn_task_type_t;

/*! callback prototype for \ref TASK_TYPE_COMPUTE */
typedef void (*dsn_task_handler_t)(void * ///< void* context
                                   );

/*! callback prototype for \ref TASK_TYPE_RPC_REQUEST */
typedef void (*dsn_rpc_request_handler_t)(dsn_message_t, ///< incoming request
                                          void *         ///< handler context registered
                                          );

/*! callback prototype for \ref TASK_TYPE_RPC_RESPONSE */
typedef void (*dsn_rpc_response_handler_t)(dsn_error_t, ///< usually, it is ok, or timeout, or busy
                                           dsn_message_t, ///< sent rpc request
                                           dsn_message_t, ///< incoming rpc response
                                           void *         ///< context when rpc is called
                                           );

/*! callback prototype for \ref TASK_TYPE_AIO */
typedef void (*dsn_aio_handler_t)(dsn_error_t, ///< error code for the io operation
                                  size_t,      ///< transferred io size
                                  void *       ///< context when rd/wt is called
                                  );

/*! task priority */
typedef enum dsn_task_priority_t {
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_COMMON,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_COUNT,
    TASK_PRIORITY_INVALID
} dsn_task_priority_t;

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

/*! register a new task code */
extern DSN_API dsn_task_code_t dsn_task_code_register(const char *name, // task code name
                                                      dsn_task_type_t type,
                                                      dsn_task_priority_t,
                                                      int pool // in which thread pool the tasks run
                                                      );
extern DSN_API void dsn_task_code_query(dsn_task_code_t code,
                                        /*out*/ dsn_task_type_t *ptype,
                                        /*out*/ dsn_task_priority_t *ppri,
                                        /*out*/ dsn::threadpool_code *ppool);
extern DSN_API void dsn_task_code_set_threadpool( // change thread pool for this task code
    dsn_task_code_t code,
    dsn::threadpool_code pool);
extern DSN_API void dsn_task_code_set_priority(dsn_task_code_t code, dsn_task_priority_t pri);
extern DSN_API const char *dsn_task_code_to_string(dsn_task_code_t code);
extern DSN_API dsn_task_code_t dsn_task_code_from_string(const char *s,
                                                         dsn_task_code_t default_code);
extern DSN_API int dsn_task_code_max();
extern DSN_API const char *dsn_task_type_to_string(dsn_task_type_t tt);
extern DSN_API const char *dsn_task_priority_to_string(dsn_task_priority_t tt);

/*!
apps updates the value at dsn_task_queue_virtual_length_ptr(..) to control
the length of a vitual queue (bound to current code + hash) to
enable customized throttling, see spec of thread pool for more information
*/
extern DSN_API volatile int *dsn_task_queue_virtual_length_ptr(dsn_task_code_t code,
                                                               int hash DEFAULT(0));

/*@}*/
