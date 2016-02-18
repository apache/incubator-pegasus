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

# pragma once

# include <dsn/c/api_common.h>

# ifdef __cplusplus
extern "C" {
# endif

/*!
@defgroup exec-model Programming Model
@ingroup dev-layer1-models

The programming and execution model for rDSN applications.

rDSN adopts the event-driven programming model, where all computations are 
represented as a task/event, which is the execution of a sequential piece of code in one thread.
Specifically, rDSN categorizes the tasks into four types, as defined in \ref dsn_task_type_t.

Unlike the traditional event-driven programming, rDSN enhances the model in the following ways,
with which they control the application in many aspects in a declarative approach.

- each task is labeled with a task code, with which developers can configure many aspects in config files. 
  Developers can define new task code using \ref DEFINE_TASK_CODE, or \ref dsn_task_code_register.

  <PRE>
  [task..default]
  ; allow task executed in other thread pools or tasks 
  ; for TASK_TYPE_COMPUTE - allow-inline allows a task being executed in its caller site 
  ; for other tasks - allow-inline allows a task being execution in io-thread
  allow_inline = false

  ; group rpc mode with group address: GRPC_TO_LEADER, GRPC_TO_ALL, GRPC_TO_ANY
  grpc_mode = GRPC_TO_LEADER

  ; when toollet profiler is enabled
  is_profile = true

  ; when toollet tracer is enabled
  is_trace = true

  ; thread pool to execute the task
  pool_code = THREAD_POOL_DEFAULT

  ; task priority
  priority = TASK_PRIORITY_COMMON

  ; whether to randomize the timer delay to random(0, timer_interval), 
  ; if the initial delay is zero, to avoid multiple timers executing at the same time (e.g., checkpointing)
  randomize_timer_delay_if_zero = false

  ; what kind of network channel for this kind of rpc calls
  rpc_call_channel = RPC_CHANNEL_TCP

  ; what kind of header format for this kind of rpc calls
  rpc_call_header_format = NET_HDR_DSN

  ; how many milliseconds to delay recving rpc session for 
  ; when queue length ~= [1.0, 1.2, 1.4, 1.6, 1.8, >=2.0] x pool.queue_length_throttling_threshold,
  ; e.g., 0, 0, 1, 2, 5, 10
  rpc_request_delays_milliseconds = 0, 0, 1, 2, 5, 10

  ; whether to drop a request right before execution when its queueing time 
  ; is already greater than its timeout value
  rpc_request_dropped_before_execution_when_timeout = false

  ; for how long (ms) the request will be resent if no response 
  ; is received yet, 0 for disable this feature
  rpc_request_resend_timeout_milliseconds = 0

  ; throttling mode for rpc requets: TM_NONE, TM_REJECT, TM_DELAY when 
  ; queue length > pool.queue_length_throttling_threshold
  rpc_request_throttling_mode = TM_NONE

  ; what is the default timeout (ms) for this kind of rpc calls
  rpc_timeout_milliseconds = 5000

  [task.LPC_AIO_IMMEDIATE_CALLBACK]
  ; override the option in [task..default]
  allow_inline = true
  </PRE>

- each task code is bound to a thread pool, which can be customized as follows.
  Developers can define new thread pools using \ref DEFINE_THREAD_POOL_CODE, or \ref dsn_threadpool_code_register.

  <PRE>
  [threadpool..default]

  ; how many tasks (if available) should be returned for
  ; one dequeue call for best batching performance
  dequeue_batch_size = 5

  ; throttling: whether to enable throttling with virtual queues
  enable_virtual_queue_throttling = false

  ; thread pool name
  name = THREAD_POOL_INVALID

  ; whethe the threads share a single queue(partitioned=false) or not;
  ; the latter is usually for workload hash partitioning for avoiding locking
  partitioned = false

  ; task queue aspects names, usually for tooling purpose
  queue_aspects =

  ; task queue provider name
  queue_factory_name = dsn::tools::hpc_concurrent_task_queue

  ; throttling: throttling threshold above which rpc requests will be dropped
  queue_length_throttling_threshold = 1000000

  ; what CPU cores are assigned to this pool, 0 for all
  worker_affinity_mask = 0

  ; task aspects names, usually for tooling purpose
  worker_aspects =

  ; thread/worker count
  worker_count = 2

  ; task worker provider name
  worker_factory_name =

  ; thread priority
  worker_priority = THREAD_xPRIORITY_NORMAL

  ; whether the threads share all assigned cores
  worker_share_core = true
  
  [threadpool.THREAD_POOL_DEFAULT]
  ; override default options in [threadpool..default]
  dequeue_batch_size = 5

  </PRE>
- 

@{
 */

/*! task/event type definition */
typedef enum dsn_task_type_t
{
    TASK_TYPE_RPC_REQUEST,   ///< task handling rpc request
    TASK_TYPE_RPC_RESPONSE,  ///< task handling rpc response or timeout
    TASK_TYPE_COMPUTE,       ///< async calls or timers
    TASK_TYPE_AIO,           ///< callback for file read and write
    TASK_TYPE_CONTINUATION,  ///< above tasks are seperated into several continuation
                             ///< tasks by thread-synchronization operations.
                             ///< so that each "task" is non-blocking
    TASK_TYPE_COUNT,
    TASK_TYPE_INVALID,
} dsn_task_type_t;

/*! callback prototype for \ref TASK_TYPE_COMPUTE */
typedef void(*dsn_task_handler_t)(
    void* ///< void* context
    );

/*! callback prototype for \ref TASK_TYPE_RPC_REQUEST */
typedef void(*dsn_rpc_request_handler_t)(
    dsn_message_t,  ///< incoming request
    void*           ///< handler context registered
    );

/*! callback prototype for \ref TASK_TYPE_RPC_RESPONSE */
typedef void(*dsn_rpc_response_handler_t)(
    dsn_error_t,    ///< usually, it is ok, or timeout, or busy
    dsn_message_t,  ///< sent rpc request
    dsn_message_t,  ///< incoming rpc response
    void*           ///< context when rpc is called
    );

/*! callback prototype for \ref TASK_TYPE_AIO */
typedef void(*dsn_aio_handler_t)(
    dsn_error_t,    ///< error code for the io operation
    size_t,         ///< transferred io size
    void*           ///< context when rd/wt is called
    );

/*! task priority */
typedef enum dsn_task_priority_t
{
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_COMMON,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_COUNT,
    TASK_PRIORITY_INVALID,
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
typedef void(*dsn_task_cancelled_handler_t)(
    void* ///< shared with the task handler callbacks, e.g., in \ref dsn_task_handler_t
    );

/*! define a new thread pool with a given name */
extern DSN_API dsn_threadpool_code_t dsn_threadpool_code_register(const char* name);
extern DSN_API const char*           dsn_threadpool_code_to_string(dsn_threadpool_code_t pool_code);
extern DSN_API dsn_threadpool_code_t dsn_threadpool_code_from_string(
                                        const char* s, 
                                        dsn_threadpool_code_t default_code // when s is not registered
                                        );
extern DSN_API int                   dsn_threadpool_code_max();
extern DSN_API int                   dsn_threadpool_get_current_tid();

/*! register a new task code */
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

/*!
apps updates the value at dsn_task_queue_virtual_length_ptr(..) to control
the length of a vitual queue (bound to current code + hash) to
enable customized throttling, see spec of thread pool for more information
*/
extern DSN_API volatile int*         dsn_task_queue_virtual_length_ptr(
                                        dsn_task_code_t code,
                                        int hash DEFAULT(0)
                                        );

/*@}*/

# ifdef __cplusplus
}
# endif