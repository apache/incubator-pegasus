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
 *     the tracer toollets traces all the asynchonous execution flow
 *     in the system through the join-point mechanism
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/c/api_common.h>

# ifdef __cplusplus
extern "C" {
# endif


// all computation in rDSN are as tasks or events, 
// i.e., in event-driven programming
// all kinds of task callbacks, see dsn_task_type_t below
typedef void(*dsn_task_handler_t)(void*); // void* context
typedef void(*dsn_rpc_request_handler_t)(
    dsn_message_t,  // incoming request
    void*           // handler context registered
    );
typedef void(*dsn_rpc_response_handler_t)(
    dsn_error_t,    // usually, it is ok, or timeout, or busy
    dsn_message_t,  // sent rpc request
    dsn_message_t,  // incoming rpc response
    void*           // context when rpc is called
    );
typedef void(*dsn_aio_handler_t)(
    dsn_error_t,    //
    size_t,         // transferred io size
    void*           // context when rd/wt is called
    );


typedef enum dsn_task_type_t
{
    TASK_TYPE_RPC_REQUEST,   // task handling rpc request
    TASK_TYPE_RPC_RESPONSE,  // task handling rpc response or timeout
    TASK_TYPE_COMPUTE,       // async calls or timers
    TASK_TYPE_AIO,           // callback for file read and write
    TASK_TYPE_CONTINUATION,  // above tasks are seperated into several continuation
                             // tasks by thread-synchronization operations.
                             // so that each "task" is non-blocking
    TASK_TYPE_COUNT,
    TASK_TYPE_INVALID,
} dsn_task_type_t;

typedef enum dsn_task_priority_t
{
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_COMMON,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_COUNT,
    TASK_PRIORITY_INVALID,
} dsn_task_priority_t;


//
// tasks can be cancelled. For languages such as C++, when there are explicit
// resource release operations (e.g., ::free, release_ref()) in the task handlers,
// cancellation will cause resource leak due to not-executed task handleers.
// in order to support such scenario, rdsn provide dsn_task_cancelled_handler_t which
// is executed when a task is cancelled. Note this callback does not have thread affinity
// similar to task handlers above (which are configured to be executed in certain thread
// pools or even a fixed thread). Therefore, it is developers' resposibility to ensure
// this cancallation callback only does thread-insensitive operations (e.g., release_ref()).
//
// the void* context is shared with the context to the task handlers above
//
typedef void(*dsn_task_cancelled_handler_t)(void*);

//------------------------------------------------------------------------------
//
// common utilities
//
//------------------------------------------------------------------------------
extern DSN_API dsn_error_t           dsn_error_register(const char* name);
extern DSN_API const char*           dsn_error_to_string(dsn_error_t err);
extern DSN_API dsn_error_t           dsn_error_from_string(const char* s, dsn_error_t default_err);
// apps updates the value at dsn_task_queue_virtual_length_ptr(..) to control
// the length of a vitual queue (bound to current code + hash) to 
// enable customized throttling, see spec of thread pool for more information
extern DSN_API volatile int*         dsn_task_queue_virtual_length_ptr(
                                        dsn_task_code_t code,
                                        int hash DEFAULT(0)
                                        );
extern DSN_API dsn_threadpool_code_t dsn_threadpool_code_register(const char* name);
extern DSN_API const char*           dsn_threadpool_code_to_string(dsn_threadpool_code_t pool_code);
extern DSN_API dsn_threadpool_code_t dsn_threadpool_code_from_string(
                                        const char* s, 
                                        dsn_threadpool_code_t default_code // when s is not registered
                                        );
extern DSN_API int                   dsn_threadpool_code_max();
extern DSN_API int                   dsn_threadpool_get_current_tid();
extern DSN_API dsn_task_code_t       dsn_task_code_register(
                                        const char* name,          // task code name
                                        dsn_task_type_t type,
                                        dsn_task_priority_t, 
                                        dsn_threadpool_code_t pool // in which thread pool the tasks run
                                        );
extern DSN_API void                  dsn_task_code_query(
                                        dsn_task_code_t code, 
                                        /*out*/ dsn_task_type_t *ptype, 
                                        /*out*/ dsn_task_priority_t *ppri, 
                                        /*out*/ dsn_threadpool_code_t *ppool
                                        );
extern DSN_API void                  dsn_task_code_set_threadpool( // change thread pool for this task code
                                        dsn_task_code_t code, 
                                        dsn_threadpool_code_t pool
                                        );
extern DSN_API void                  dsn_task_code_set_priority(dsn_task_code_t code, dsn_task_priority_t pri);
extern DSN_API const char*           dsn_task_code_to_string(dsn_task_code_t code);
extern DSN_API dsn_task_code_t       dsn_task_code_from_string(const char* s, dsn_task_code_t default_code);
extern DSN_API int                   dsn_task_code_max();
extern DSN_API const char*           dsn_task_type_to_string(dsn_task_type_t tt);
extern DSN_API const char*           dsn_task_priority_to_string(dsn_task_priority_t tt);
extern DSN_API bool                  dsn_task_is_running_inside(dsn_task_t t); // is inside given task


//------------------------------------------------------------------------------
//
// tasking - asynchronous tasks and timers tasks executed in target thread pools
//
// use the config-dump command in rDSN cli for detailed configurations for
// each kind of task
//
// all returned dsn_task_t are NOT add_ref by rDSN,
// so you DO NOT need to call task_release_ref to release the tasks.
// the decision is made for easier programming, and you may consider the later
// dsn_rpc_xxx calls do the resource gc work for you.
//
// however, before you emit the tasks (e.g., via dsn_task_call, dsn_rpc_call),
// AND you want to hold the task handle further after the emit API,
// you need to call dsn_task_add_ref to ensure the handle is 
// still valid, and also call dsn_task_release_ref later to 
// release the handle.
//
extern DSN_API void        dsn_task_release_ref(dsn_task_t task);
extern DSN_API void        dsn_task_add_ref(dsn_task_t task);
extern DSN_API int         dsn_task_get_ref(dsn_task_t task);

//
// task trackers are used to track task context
//
// when a task executes, it usually accesses certain context
// when the context is gone, all tasks accessing this context needs 
// to be cancelled automatically to avoid invalid context access
// 
// to release this burden from developers, rDSN provides 
// task tracker which can be embedded into a context, and
// destroyed when the context is gone
//
extern DSN_API dsn_task_tracker_t dsn_task_tracker_create(int task_bucket_count);
extern DSN_API void               dsn_task_tracker_destroy(dsn_task_tracker_t tracker);
extern DSN_API void               dsn_task_tracker_cancel_all(dsn_task_tracker_t tracker);
extern DSN_API void               dsn_task_tracker_wait_all(dsn_task_tracker_t tracker);

// create a common asynchronous task
// - code defines the thread pool which executes the callback
//   i.e., [task.%code$] pool_code = THREAD_POOL_DEFAULT
// - hash defines the thread with index hash % worker_count in the threadpool
//   to execute the callback, when [threadpool.%pool_code%] partitioned = true
//   
extern DSN_API dsn_task_t  dsn_task_create(
                            dsn_task_code_t code,               // task label
                            dsn_task_handler_t cb,              // callback function
                            void* context,                      // context to the callback
                            int hash DEFAULT(0), // hash to callback
                            dsn_task_tracker_t tracker DEFAULT(nullptr)
                            );
extern DSN_API dsn_task_t  dsn_task_create_timer(
                            dsn_task_code_t code, 
                            dsn_task_handler_t cb, 
                            void* context, 
                            int hash,
                            int interval_milliseconds,         // timer period
                            dsn_task_tracker_t tracker DEFAULT(nullptr)
                            );
// repeated declarations later in correpondent rpc and file sections
//extern DSN_API dsn_task_t  dsn_rpc_create_response_task(...);
//extern DSN_API dsn_task_t  dsn_file_create_aio_task(...);

//
// task create api with on_cancel callback, see comments for 
// dsn_task_cancelled_handler_t for details.
//
extern DSN_API dsn_task_t  dsn_task_create_ex(
    dsn_task_code_t code,               // task label
    dsn_task_handler_t cb,              // callback function
    dsn_task_cancelled_handler_t on_cancel, 
    void* context,                      // context to the two callbacks above
    int hash DEFAULT(0), // hash to callback
    dsn_task_tracker_t tracker DEFAULT(nullptr)
    );
extern DSN_API dsn_task_t  dsn_task_create_timer_ex(
    dsn_task_code_t code,
    dsn_task_handler_t cb,
    dsn_task_cancelled_handler_t on_cancel,
    void* context,
    int hash,
    int interval_milliseconds,         // timer period
    dsn_task_tracker_t tracker DEFAULT(nullptr)
    );
// repeated declarations later in correpondent rpc and file sections
//extern DSN_API dsn_task_t  dsn_rpc_create_response_task_ex(...);
//extern DSN_API dsn_task_t  dsn_file_create_aio_task_ex(...);

//
// common task 
// - task: must be created by dsn_task_create or dsn_task_create_timer
// - tracker: can be null. 
//
extern DSN_API void        dsn_task_call(
                                dsn_task_t task,                                 
                                int delay_milliseconds DEFAULT(0)
                                );
extern DSN_API bool        dsn_task_cancel(dsn_task_t task, bool wait_until_finished);
extern DSN_API bool        dsn_task_cancel2(
                                dsn_task_t task, 
                                bool wait_until_finished, 
                                /*out*/ bool* finished
                                );
extern DSN_API void        dsn_task_cancel_current_timer();
extern DSN_API bool        dsn_task_wait(dsn_task_t task); 
extern DSN_API bool        dsn_task_wait_timeout(
                                dsn_task_t task,
                                int timeout_milliseconds
                                );
extern DSN_API dsn_error_t dsn_task_error(dsn_task_t task);


# ifdef __cplusplus
}
# endif