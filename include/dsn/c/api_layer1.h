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
 *     Service API  in rDSN
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), add comments for V1 release
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/c/api_common.h>
#include <dsn/c/api_task.h>

/*!
 @defgroup service-api-c Core Service API

 @ingroup service-api

  Core service API for building applications and distributed frameworks, which
  covers the major categories that a server application may use, shown in the modules below.

 @{
 */

/*!
 @defgroup task-common Common Task Operations

 Common Task/Event Operations

 rDSN adopts the event-driven programming model, where all computations (event handlers) are
 represented as individual tasks; each is the execution of a sequential piece of code in one thread.
 Specifically, rDSN categorizes the tasks into four types, as defined in \ref dsn_task_type_t.

Unlike the traditional event-driven programming, rDSN enhances the model in the following ways,
with which they control the application in many aspects in a declarative approach.

- each task is labeled with a task code, with which developers can configure many aspects in config
files.
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
  ; if the initial delay is zero, to avoid multiple timers executing at the same time (e.g.,
checkpointing)
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
  Developers can define new thread pools using \ref DEFINE_THREAD_POOL_CODE, or \ref
dsn_threadpool_code_register.

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

/*!
 Add reference count for a task created from \ref dsn_task_create etc.

 \param task the task handle.

 Memory usage of tasks are controlled using reference-count. All returned dsn_task_t
 are NOT add_ref by rDSN, so you DO NOT need to call task_release_ref to release the
 tasks. the decision is made for easier programming, and you may consider the later
 dsn_rpc_xxx calls do the resource gc work for you.

 however, before you emit the tasks (e.g., via dsn_task_call, dsn_rpc_call), AND you
 want to hold the task handle further after the emit API, you need to call
 dsn_task_add_ref to ensure the handle is still valid, and also call
 dsn_task_release_ref later to release the handle.
 */
extern DSN_API void dsn_task_add_ref(dsn_task_t task);

/*!
 release reference for a given task handle

 \param task the task handle

 See more details of the comment in \ref dsn_task_add_ref
 */
extern DSN_API void dsn_task_release_ref(dsn_task_t task);

/*!
 get reference for a given task handle

 \param task the task handle

 See more details of the comment in \ref dsn_task_add_ref
*/
extern DSN_API int dsn_task_get_ref(dsn_task_t task);

/*!
 cancel a task

 \param task                the task handle
 \param wait_until_finished true if wait until finished is needed

 \return true if THIS cancellation succeeds, false if the task is already running
 (when wait_until_finished = false), or completed successfully, or already cancelled.
 */
extern DSN_API bool dsn_task_cancel(dsn_task_t task, bool wait_until_finished);

/*!
 set delay for a task

 \param task                the task handle
 \param delay_ms            the delay milliseconds for a task
 */
extern DSN_API void dsn_task_set_delay(dsn_task_t task, int delay_ms);

/*!
 cancel a task

 \param task                the task handle
 \param wait_until_finished true if wait until finished is needed
 \param finished            after the call, whether the task is finished
 (completed successfully, or cancelled)

 \return true if THIS cancellation succeeds, false if the task is already running
 (when wait_until_finished = false), or completed successfully, or cancelled.
 */
extern DSN_API bool dsn_task_cancel2(dsn_task_t task,
                                     bool wait_until_finished,
                                     /*out*/ bool *finished);

/*! cancel the later execution of the timer task inside the timer */
extern DSN_API void dsn_task_cancel_current_timer();

/*!
 wait until a task is completed

 \param task the task handle

 \return true if it succeeds, false if it fails.
 */
extern DSN_API void dsn_task_wait(dsn_task_t task);

/*!
wait until a task is completed

\param task the task handle
\param timeout_milliseconds maximum time to wait

\return true if it succeeds, false if it timeouts
*/
extern DSN_API bool dsn_task_wait_timeout(dsn_task_t task, int timeout_milliseconds);

/*!
 get result error code of a task

 \param task the task handle.

 \return the result error code of the task
 */
extern DSN_API dsn::error_code dsn_task_error(dsn_task_t task);

/*!
 check whether the task is currently running inside the given task

 \param t the given task handle

 \return true if it is.
 */
extern DSN_API bool dsn_task_is_running_inside(dsn_task_t t);

/*!
 task trackers are used to track task context

 When a task executes, it usually accesses certain context. When the context is gone, all tasks
 accessing this context needs to be cancelled automatically to avoid invalid context access.
 To release this burden from developers, rDSN provides task tracker which can be embedded into
 a context, and destroyed when the context is gone.

 \param task_bucket_count number of task buckets to reduce thread conflicts

 \return task tracker handle
 */
extern DSN_API dsn_task_tracker_t dsn_task_tracker_create(int task_bucket_count);

/*!
 destroy a task tracker, which cancels all pending tasks as well

 \param tracker task tracker handle
 */
extern DSN_API void dsn_task_tracker_destroy(dsn_task_tracker_t tracker);

/*!
cancels all pending tasks bound to this tracker

\param tracker task tracker handle
*/
extern DSN_API void dsn_task_tracker_cancel_all(dsn_task_tracker_t tracker);

/*!
wait all pending tasks to be completed bound to this tracker

\param tracker task tracker handle
*/
extern DSN_API void dsn_task_tracker_wait_all(dsn_task_tracker_t tracker);

/*@}*/

/*!
 @defgroup tasking Asynchronous Tasks and Timers

 Asynchronous Tasks and Timers

 @{
 */

/*!
 create an asynchronous task.

 \param code
 the task code, which defines which thread pool executes the task, see
 \ref dsn_task_code_register for more details.
 \param cb               the callback for executing the task.
 \param context          the context used by the callback.
 \param DEFAULT(0)
    the hash value, which defines which thread in the target thread pool
    executes the task, when the pool is partitioned, see remarks for more.
 \param DEFAULT(nullptr) the task tracker handle, see \ref dsn_task_tracker_create for more.

 \return task handle

 code defines the thread pool which executes the callback, i.e., [task.%code$] pool_code =
 THREAD_POOL_DEFAULT; hash defines the thread with index hash % worker_count in the
 threadpool to execute the callback, when [threadpool.%pool_code%] partitioned = true.
 */
extern DSN_API dsn_task_t dsn_task_create(dsn_task_code_t code,
                                          dsn_task_handler_t cb,
                                          void *context,
                                          int hash DEFAULT(0),
                                          dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
 create a timer task

 \param code
 the task code, which defines which thread pool executes the task, see
 \ref dsn_task_code_register for more details.
 \param cb               the callback for executing the task.
 \param context          the context used by the callback.
 \param DEFAULT(0)
    the hash value, which defines which thread in the target thread pool
    executes the task, when the pool is partitioned, see remarks for more.
 \param interval_milliseconds timer interval with which the timer executes periodically.
 \param DEFAULT(nullptr) the task tracker handle, see \ref dsn_task_tracker_create for more.

 \return task handle

 code defines the thread pool which executes the callback, i.e., [task.%code$] pool_code =
 THREAD_POOL_DEFAULT; hash defines the thread with index hash % worker_count in the
 threadpool to execute the callback, when [threadpool.%pool_code%] partitioned = true.
 */
extern DSN_API dsn_task_t dsn_task_create_timer(dsn_task_code_t code,
                                                dsn_task_handler_t cb,
                                                void *context,
                                                int hash,
                                                int interval_milliseconds,
                                                dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
similar to \ref dsn_task_create, except an on_cancel callback is provided
to be executed when the task is cancelled, see \ref dsn_task_cancelled_handler_t for
more details.
*/
extern DSN_API dsn_task_t dsn_task_create_ex(dsn_task_code_t code,  // task label
                                             dsn_task_handler_t cb, // callback function
                                             dsn_task_cancelled_handler_t on_cancel,
                                             void *context, // context to the two callbacks above
                                             int hash DEFAULT(0), // hash to callback
                                             dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
 similar to \ref dsn_task_create_timer, except an on_cancel callback is provided
 to be executed when the task is cancelled, see \ref dsn_task_cancelled_handler_t for
 more details.
 */
extern DSN_API dsn_task_t dsn_task_create_timer_ex(dsn_task_code_t code,
                                                   dsn_task_handler_t cb,
                                                   dsn_task_cancelled_handler_t on_cancel,
                                                   void *context,
                                                   int hash,
                                                   int interval_milliseconds, // timer period
                                                   dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
 start the task

 \param task                the task handle
 \param delay_milliseconds  delay time before its execution
 */
extern DSN_API void dsn_task_call(dsn_task_t task, int delay_milliseconds DEFAULT(0));
/*@}*/

/*!
@defgroup rpc Remote Procedure Call (RPC)

Remote Procedure Call (RPC)

Note developers can easily plugin their own implementation to
replace the underneath implementation of the network (e.g., RDMA, simulated network)
@{
*/

/*!
@defgroup rpc-addr RPC Address Utilities

RPC Address Utilities

@{
*/

/*! rpc address host type */
typedef enum dsn_host_type_t {
    HOST_TYPE_INVALID = 0,
    HOST_TYPE_IPV4 = 1,  ///< 4 bytes for IPv4
    HOST_TYPE_GROUP = 2, ///< reference to an address group object
    HOST_TYPE_URI = 3,   ///< universal resource identifier as a string
} dsn_host_type_t;

/*! rpc address, which is always encoded into a 64-bit integer */
typedef struct dsn_address_t
{
    union u_t
    {
        struct
        {
            unsigned long long type : 2;
            unsigned long long padding : 14;
            unsigned long long port : 16;
            unsigned long long ip : 32;
        } v4; ///< \ref HOST_TYPE_IPV4
        struct
        {
            unsigned long long type : 2;
            unsigned long long uri : 62;
        } uri; ///< \ref HOST_TYPE_URI
        struct
        {
            unsigned long long type : 2;
            unsigned long long group : 62; ///< dsn_group_t
        } group;                           ///< \ref HOST_TYPE_GROUP
        uint64_t value;
    } u;
} dsn_address_t;

/*! translate from hostname to ipv4 in host machine order */
extern DSN_API uint32_t dsn_ipv4_from_host(const char *name);

/*! get local ipv4 according to the given network interface name */
extern DSN_API uint32_t dsn_ipv4_local(const char *network_interface);

/*! build a RPC address from given host name or IPV4 string, and port */
extern DSN_API dsn_address_t dsn_address_build(const char *host, uint16_t port);

/*! build a RPC address from a given ipv4 in host machine order and port */
extern DSN_API dsn_address_t dsn_address_build_ipv4(uint32_t ipv4, uint16_t port);

/*! build a RPC address from a group address (created using \ref dsn_group_build) */
extern DSN_API dsn_address_t dsn_address_build_group(dsn_group_t g);

/*! build a RPC address from a URI address (created using \ref dsn_uri_build) */
extern DSN_API dsn_address_t dsn_address_build_uri(dsn_uri_t uri);

/*! dump a RPC address to a meaningful string for logging purpose */
extern DSN_API const char *dsn_address_to_string(dsn_address_t addr);

/*! build URI address from a string URL, must be destroyed later using \ref dsn_uri_destroy */
extern DSN_API dsn_uri_t dsn_uri_build(const char *url);

/*! build URI address from another, must be destroyed later using \ref dsn_uri_destroy */
extern DSN_API dsn_uri_t dsn_uri_clone(dsn_uri_t uri);

/*! destroy a URI address */
extern DSN_API void dsn_uri_destroy(dsn_uri_t uri);

/*! build a group address with a name, must be destroyed later using \ref dsn_group_destroy */
extern DSN_API dsn_group_t dsn_group_build(const char *name);

/*! clone a group address from another, must be destroyed later using \ref dsn_group_destroy */
extern DSN_API dsn_group_t dsn_group_clone(dsn_group_t g);

/*! get the RPC address count contained in the group address */
extern DSN_API int dsn_group_count(dsn_group_t g);

/*! add an RPC address into the group address */
extern DSN_API bool dsn_group_add(dsn_group_t g, dsn_address_t ep);

/*! remove an RPC address into the group address */
extern DSN_API bool dsn_group_remove(dsn_group_t g, dsn_address_t ep);

/*! set an RPC address as the leader in the group address */
extern DSN_API void dsn_group_set_leader(dsn_group_t g, dsn_address_t ep);

/*! get leader from the group address */
extern DSN_API dsn_address_t dsn_group_get_leader(dsn_group_t g);

/*! check whether the given endpoint is the leader in the group */
extern DSN_API bool dsn_group_is_leader(dsn_group_t g, dsn_address_t ep);

/*! whether auto-update of the leader in rDSN runtime is allowed, default is true */
extern DSN_API bool dsn_group_is_update_leader_automatically(dsn_group_t g);

/*! set auto-update mode of the leader in rDSN runtime for this group address, true for yes */
extern DSN_API void dsn_group_set_update_leader_automatically(dsn_group_t g, bool v);

/*! get the next address in the group right after (circularly) given ep, if ep is invalid, a random
 * member is returned */
extern DSN_API dsn_address_t dsn_group_next(dsn_group_t g, dsn_address_t ep);

/*! set the next address after (circularly) the current leader as the group leader */
extern DSN_API dsn_address_t dsn_group_forward_leader(dsn_group_t g);

/*! destroy the group address object */
extern DSN_API void dsn_group_destroy(dsn_group_t g);

/*! get the primary address of the rpc engine attached to the current thread */
extern DSN_API dsn_address_t dsn_primary_address();

/*@}*/

/*!
@defgroup rpc-msg RPC Message Utilities

RPC Message Utilities


rpc message and buffer management

all returned dsn_message_t are NOT add_ref by rDSN (unless explicitly specified), so you
do not need to call msg_release_ref to release the msgs.  the decision is made for easier
programming, and you may consider the later dsn_rpc_xxx calls do the resource gc work for
you.  however, if you want to hold the message further after call dsn_rpc_xxx, you need to
call dsn_msg_add_ref first before these operations,  and call dsn_msg_release_ref later to
ensure there is no memory leak.  This is very similar to what we have above with task
handles.  however, this is not true for returned message from dsn_rpc_call_wait and
dsn_rpc_get_response. For these two cases,  developers are responsible for releasing the
message handle  by calling dsn_msg_release_ref. similarily, for all msgs accessable in
callbacks, if you want to hold them in  upper apps further beyond the callbacks,  you need to
call msg_add_ref, and msg_release_ref explicitly.  when timeout_milliseconds == 0,
[task.%rpc_code%] rpc_timeout_milliseconds is used.

rpc message read/write

<PRE>
// apps write rpc message as follows:
       void* ptr;
       size_t size;
       dsn_msg_write_next(msg, &ptr, &size, min_size);
       write msg content to [ptr, ptr + size)
       dsn_msg_write_commit(msg, real_written_size);

// apps read rpc message as follows:
       void* ptr;
       size_t size;
       dsn_msg_read_next(msg, &ptr, &size);
       read msg content in [ptr, ptr + size)
       dsn_msg_read_commit(msg, real read size);
// if not committed, next dsn_msg_read_next returns the same read buffer
</PRE>

@{
*/

/*!
 create a rpc request message

 \param rpc_code              task code for this request
 \param timeout_milliseconds  timeout for the RPC call, 0 for default value as
                              configued in config files for the task code
 \param thread_hash           used for thread dispatching on server,
                              if thread_hash == 0 && partition_hash != 0, thread_hash is computed
 from partition_hash
 \param partition_hash        used for finding which partition the request should be sent to
 \return RPC message handle
 */
extern DSN_API dsn_message_t dsn_msg_create_request(dsn_task_code_t rpc_code,
                                                    int timeout_milliseconds DEFAULT(0),
                                                    int thread_hash DEFAULT(0),
                                                    uint64_t partition_hash DEFAULT(0));

/*! create a RPC response message correspondent to the given request message */
extern DSN_API dsn_message_t dsn_msg_create_response(dsn_message_t request);

/*! make a copy of the given message */
extern DSN_API dsn_message_t dsn_msg_copy(dsn_message_t msg,
                                          bool clone_content,
                                          bool copy_for_receive);

/*! add reference to the message, paired with /ref dsn_msg_release_ref */
extern DSN_API void dsn_msg_add_ref(dsn_message_t msg);

/*! release reference to the message, paired with /ref dsn_msg_add_ref */
extern DSN_API void dsn_msg_release_ref(dsn_message_t msg);

/*! define various serialization format supported by rDSN, note any changes here must also be
 * reflected in src/tools/.../dsn_transport.js */
typedef enum dsn_msg_serialize_format {
    DSF_INVALID = 0,
    DSF_THRIFT_BINARY = 1,
    DSF_THRIFT_COMPACT = 2,
    DSF_THRIFT_JSON = 3,
    DSF_PROTOC_BINARY = 4,
    DSF_PROTOC_JSON = 5,
    DSF_JSON = 6
} dsn_msg_serialize_format;

/*! explicitly create a received RPC request, MUST released mannually later using
 * dsn_msg_release_ref */
extern DSN_API dsn_message_t
dsn_msg_create_received_request(dsn_task_code_t rpc_code,
                                dsn_msg_serialize_format serialization_type,
                                void *buffer,
                                int size,
                                int thread_hash DEFAULT(0),
                                uint64_t partition_hash DEFAULT(0));

/*! type of the parameter in \ref dsn_msg_context_t */
typedef enum dsn_msg_parameter_type_t {
    MSG_PARAM_NONE = 0, ///< nothing
} dsn_msg_parameter_type_t;

/*! RPC message context */
typedef union dsn_msg_context_t
{
    struct
    {
        uint64_t is_request : 1;           ///< whether the RPC message is a request or response
        uint64_t is_forwarded : 1;         ///< whether the msg is forwarded or not
        uint64_t unused : 4;               ///< not used yet
        uint64_t serialize_format : 4;     ///< dsn_msg_serialize_format
        uint64_t is_forward_supported : 1; ///< whether support forwarding a message to real leader
        uint64_t
            parameter_type : 3;  ///< type of the parameter next, see \ref dsn_msg_parameter_type_t
        uint64_t parameter : 50; ///< piggybacked parameter for specific flags above
    } u;
    uint64_t context; ///< msg_context is of sizeof(uint64_t)
} dsn_msg_context_t;

typedef union dsn_global_partition_id
{
    struct
    {
        int32_t app_id;          ///< 1-based app id (0 for invalid)
        int32_t partition_index; ///< zero-based partition index
    } u;
    uint64_t value;
} dsn_gpid;

inline int dsn_gpid_to_thread_hash(dsn_gpid gpid)
{
    return gpid.u.app_id * 7919 + gpid.u.partition_index;
}

#define DSN_MSGM_TIMEOUT (0x1 << 0)        ///< msg timeout is to be set/get
#define DSN_MSGM_THREAD_HASH (0x1 << 1)    ///< thread hash is to be set/get
#define DSN_MSGM_PARTITION_HASH (0x1 << 2) ///< partition hash is to be set/get
#define DSN_MSGM_VNID (0x1 << 3)           ///< virtual node id (gpid) is to be set/get
#define DSN_MSGM_CONTEXT (0x1 << 4)        ///< rpc message context is to be set/get

/*! options for RPC messages, used by \ref dsn_msg_set_options and \ref dsn_msg_get_options */
typedef struct dsn_msg_options_t
{
    int timeout_ms;  ///< RPC timeout in milliseconds
    int thread_hash; ///< thread hash on RPC server
    ///< if thread_hash == 0 && partition_hash != 0, thread_hash is computed from partition_hash
    uint64_t partition_hash;   ///< partition hash for calculating partition index
    dsn_gpid gpid;             ///< virtual node id, 0 for none
    dsn_msg_context_t context; ///< see \ref dsn_msg_context_t
} dsn_msg_options_t;

/*! make sure type sizes match as we simply use uint64_t across language boundaries */
inline void dsn_address_size_checker()
{
    static_assert(sizeof(dsn_address_t) == sizeof(uint64_t),
                  "sizeof(dsn_address_t) must equal to sizeof(uint64_t)");

    static_assert(sizeof(dsn_msg_context_t) == sizeof(uint64_t),
                  "sizeof(dsn_msg_context_t) must equal to sizeof(uint64_t)");

    static_assert(sizeof(dsn_gpid) == sizeof(uint64_t),
                  "sizeof(dsn_gpid) must equal to sizeof(uint64_t)");
}

/*!
 set options for the given message

 \param msg  the message handle
 \param opts options to be set in the message
 \param mask the mask composed using e.g., DSN_MSGM_TIMEOUT above to specify what to set
 */
extern DSN_API void dsn_msg_set_options(dsn_message_t msg, dsn_msg_options_t *opts, uint32_t mask);

/*!
 get options for the given message

 \param msg  the message handle
 \param opts options to be get
 */
extern DSN_API void dsn_msg_get_options(dsn_message_t msg,
                                        /*out*/ dsn_msg_options_t *opts);

DSN_API void dsn_msg_set_serailize_format(dsn_message_t msg, dsn_msg_serialize_format fmt);

DSN_API dsn_msg_serialize_format dsn_msg_get_serialize_format(dsn_message_t msg);

/*! get message body size */
extern DSN_API size_t dsn_msg_body_size(dsn_message_t msg);

/*! get read/write pointer with the given offset */
extern DSN_API void *dsn_msg_rw_ptr(dsn_message_t msg, size_t offset_begin);

/*! get from-address where the message is sent */
extern DSN_API dsn_address_t dsn_msg_from_address(dsn_message_t msg);

/*! get to-address where the message is sent to */
extern DSN_API dsn_address_t dsn_msg_to_address(dsn_message_t msg);

/*! get trace id of the message */
extern DSN_API uint64_t dsn_msg_trace_id(dsn_message_t msg);

/*! get task code of the message, return TASK_CODE_INVALID if code name is not recognized */
extern DSN_API dsn_task_code_t dsn_msg_task_code(dsn_message_t msg);

/*! get rpc name of the message */
extern DSN_API const char *dsn_msg_rpc_name(dsn_message_t msg);

/*!
 get message write buffer

 \param msg      message handle
 \param ptr      *ptr returns the writable memory pointer
 \param size     *size returns the writable memory buffer size
 \param min_size *size must >= min_size
 */
extern DSN_API void dsn_msg_write_next(dsn_message_t msg,
                                       /*out*/ void **ptr,
                                       /*out*/ size_t *size,
                                       size_t min_size);

/*! commit the write buffer after the message content is written with the real written size */
extern DSN_API void dsn_msg_write_commit(dsn_message_t msg, size_t size);

/*!
 get message read buffer

 \param msg  message handle
 \param ptr  *ptr points to the next read buffer
 \param size *size points to the size of the next buffer filled with content

 \return true if it succeeds, false if it is already beyond the end of the message
 */
extern DSN_API bool dsn_msg_read_next(dsn_message_t msg,
                                      /*out*/ void **ptr,
                                      /*out*/ size_t *size);

/*! commit the read buffer after the message content is read with the real read size,
    it is possible to use a different size to allow duplicated or skipped read in the message.
 */
extern DSN_API void dsn_msg_read_commit(dsn_message_t msg, size_t size);

/*@}*/

/*!
@defgroup rpc-server Server-Side RPC Primitives

Server-Side RPC Primitives
@{
 */

/*! register callback to handle RPC request */
extern DSN_API bool dsn_rpc_register_handler(dsn_task_code_t code,
                                             const char *name,
                                             dsn_rpc_request_handler_t cb,
                                             void *context,
                                             dsn_gpid gpid DEFAULT(dsn_gpid{0}));

/*! unregister callback to handle RPC request, and returns void* context upon \ref
 * dsn_rpc_register_handler  */
extern DSN_API void *dsn_rpc_unregiser_handler(dsn_task_code_t code,
                                               dsn_gpid gpid DEFAULT(dsn_gpid{0}));

/*! reply with a response which is created using dsn_msg_create_response */
extern DSN_API void dsn_rpc_reply(dsn_message_t response, dsn::error_code err DEFAULT(dsn::ERR_OK));

/*! forward the request to another server instead */
extern DSN_API void dsn_rpc_forward(dsn_message_t request, dsn_address_t addr);

/*@}*/

/*!
@defgroup rpc-client Client-Side RPC Primitives

Client-Side RPC Primitives
@{
*/

/*!
create a callback task to handle the response message from RPC server, or timeout.

\param request          rpc request message
\param cb               callback to handle rpc response or timeout, unlike the other
 kinds of tasks, response tasks are always executed in the thread pool invoking the rpc
\param context          context used by cb
\param reply_thread_hash       if the curren thread pool is partitioned, this specify which thread
 to execute the callback
\param tracker          task tracker bound to the response task

\return response task handle
*/
extern DSN_API dsn_task_t dsn_rpc_create_response_task(dsn_message_t request,
                                                       dsn_rpc_response_handler_t cb,
                                                       void *context,
                                                       int reply_thread_hash DEFAULT(0),
                                                       dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
create a callback task to handle the response message from RPC server, or timeout.

\param request          rpc request message
\param cb               callback to handle rpc response or timeout, unlike the other
 kinds of tasks, response tasks are always executed in the thread pool invoking the rpc
\param on_cancel        callback executed on task being-cancelled
\param context          context used by cb
\param reply_thread_hash       if the curren thread pool is partitioned, this specify which thread
 to execute the callback
\param tracker          task tracker bound to the response task

\return response task handle
*/
extern DSN_API dsn_task_t
dsn_rpc_create_response_task_ex(dsn_message_t request,
                                dsn_rpc_response_handler_t cb,
                                dsn_task_cancelled_handler_t on_cancel,
                                void *context,
                                int reply_thread_hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr));

/*! client invokes the RPC call */
extern DSN_API void dsn_rpc_call(dsn_address_t server, dsn_task_t rpc_call);

/*!
   client invokes the RPC call and waits for its response, note
   returned msg must be explicitly released using \ref dsn_msg_release_ref
 */
extern DSN_API dsn_message_t dsn_rpc_call_wait(dsn_address_t server, dsn_message_t request);

/*! one-way RPC from client, no rpc response is expected */
extern DSN_API void dsn_rpc_call_one_way(dsn_address_t server, dsn_message_t request);

/*!
 get response message from the response task, note
 returned msg must be explicitly released using \ref dsn_msg_release_ref
*/
extern DSN_API dsn_message_t dsn_rpc_get_response(dsn_task_t rpc_call);

/*! this is to mimic a response is received when no real rpc is called */
extern DSN_API void
dsn_rpc_enqueue_response(dsn_task_t rpc_call, dsn::error_code err, dsn_message_t response);

/*@}*/

/*@}*/

/*!
@defgroup file File Operations

File Operations

Note developers can easily plugin their own implementation to
replace the underneath implementation of these primitives.
@{
*/
typedef struct
{
    void *buffer;
    int size;
} dsn_file_buffer_t;

/*! the following ctrl code are used by \ref dsn_file_ctrl. */
typedef enum dsn_ctrl_code_t {
    CTL_BATCH_INVALID = 0,
    CTL_BATCH_WRITE = 1,            ///< (batch) set write batch size
    CTL_MAX_CON_READ_OP_COUNT = 2,  ///< (throttling) maximum concurrent read ops
    CTL_MAX_CON_WRITE_OP_COUNT = 3, ///< (throttling) maximum concurrent write ops
} dsn_ctrl_code_t;

/*!
 open file

 \param file_name filename of the file.
 \param flag      flags such as O_RDONLY | O_BINARY used by ::open
 \param pmode     permission mode used by ::open

 \return file handle
 */
extern DSN_API dsn_handle_t dsn_file_open(const char *file_name, int flag, int pmode);

/*! close the file handle */
extern DSN_API dsn::error_code dsn_file_close(dsn_handle_t file);

/*! flush the buffer of the given file */
extern DSN_API dsn::error_code dsn_file_flush(dsn_handle_t file);

/*! get native handle: HANDLE for windows, int for non-windows */
extern DSN_API void *dsn_file_native_handle(dsn_handle_t file);

/*!
 create aio task which is executed on completion of the file operations

 \param code             task code
 \param cb               callback to be executed
 \param context          context used by cb
 \param hash             specify which thread to execute cb if target pool is partitioned
 \param tracker          task tracker bound to this aio task

 \return aio task handle, nullptr for failure
 */
extern DSN_API dsn_task_t dsn_file_create_aio_task(dsn_task_code_t code,
                                                   dsn_aio_handler_t cb,
                                                   void *context,
                                                   int hash DEFAULT(0),
                                                   dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
 create aio task which is executed on completion of the file operations

 \param code             task code
 \param cb               callback to be executed on task completion
 \param on_cancel        callback to be executed on task being-cancelled
 \param context          context used by cb
 \param hash             specify which thread to execute cb if target pool is partitioned
 \param tracker          task tracker bound to this aio task

 \return aio task handle, nullptr for failure
 */
extern DSN_API dsn_task_t dsn_file_create_aio_task_ex(dsn_task_code_t code,
                                                      dsn_aio_handler_t cb,
                                                      dsn_task_cancelled_handler_t on_cancel,
                                                      void *context,
                                                      int hash DEFAULT(0),
                                                      dsn_task_tracker_t tracker DEFAULT(nullptr));

/*!
 read file asynchronously

 \param file   file handle
 \param buffer read buffer
 \param count  byte size of the read buffer
 \param offset offset in the file to start reading
 \param cb     callback aio task to be executed on completion
 */
extern DSN_API void
dsn_file_read(dsn_handle_t file, char *buffer, int count, uint64_t offset, dsn_task_t cb);

/*!
 write file asynchronously

 \param file   file handle
 \param buffer write buffer
 \param count  byte size of the to-be-written content
 \param offset offset in the file to start write
 \param cb     callback aio task to be executed on completion
 */
extern DSN_API void
dsn_file_write(dsn_handle_t file, const char *buffer, int count, uint64_t offset, dsn_task_t cb);

/*!
 write file asynchronously with vector buffers

 \param file          file handle
 \param buffers       write buffers
 \param buffer_count  number of write buffers
 \param offset        offset in the file to start write
 \param cb            callback aio task to be executed on completion
 */
extern DSN_API void dsn_file_write_vector(dsn_handle_t file,
                                          const dsn_file_buffer_t *buffers,
                                          int buffer_count,
                                          uint64_t offset,
                                          dsn_task_t cb);

/*!
 copy remote directory to the local machine

 \param remote     address of the remote nfs server
 \param source_dir source dir on remote server
 \param dest_dir   destination dir on local server
 \param overwrite  true to overwrite, false to preserve.
 \param high_priority  true means copy in high priority.
 \param cb         callback aio task to be executed on completion
 */
extern DSN_API void dsn_file_copy_remote_directory(dsn_address_t remote,
                                                   const char *source_dir,
                                                   const char *dest_dir,
                                                   bool overwrite,
                                                   bool high_priority,
                                                   dsn_task_t cb);

/*!
 copy remote files to the local machine

 \param remote       address of the remote nfs server
 \param source_dir   source dir on remote server
 \param source_files zero-ended file string array within the source dir on remote server,
    when it contains no files, all files within source_dir are copied
 \param dest_dir     destination dir on local server
 \param overwrite    true to overwrite, false to preserve.
 \param high_priority  true means copy in high priority.
 \param cb           callback aio task to be executed on completion
 */
extern DSN_API void dsn_file_copy_remote_files(dsn_address_t remote,
                                               const char *source_dir,
                                               const char **source_files,
                                               const char *dest_dir,
                                               bool overwrite,
                                               bool high_priority,
                                               dsn_task_t cb);

/*! get read/written io size for the given aio task */
extern DSN_API size_t dsn_file_get_io_size(dsn_task_t cb_task);

/*! mimic io completion when no io operation is really issued */
extern DSN_API void dsn_file_task_enqueue(dsn_task_t cb_task, dsn::error_code err, size_t size);

/*@}*/

/*!
@defgroup env Environment

Non-deterministic Environment Input

Note developers can easily plugin their own implementation to
replace the underneath implementation of these primitives.
@{
*/
extern DSN_API uint64_t dsn_runtime_init_time_ms();
extern DSN_API uint64_t dsn_now_ns();

/*! return [min, max] */
extern DSN_API uint64_t dsn_random64(uint64_t min, uint64_t max);

__inline uint64_t dsn_now_us() { return dsn_now_ns() / 1000; }
__inline uint64_t dsn_now_ms() { return dsn_now_ns() / 1000000; }

/*! return [min, max] */
__inline uint32_t dsn_random32(uint32_t min, uint32_t max)
{
    return (uint32_t)(dsn_random64(min, max));
}

__inline double dsn_probability() { return (double)(dsn_random64(0, 1000000000)) / 1000000000.0; }

/*@}*/

/*!
@defgroup sync Thread Synchornization

Thread Synchornization Primitives

Note developers can easily plugin their own implementation to
replace the underneath implementation of these primitives.
@{
*/

/*!
@defgroup sync-exlock Exlusive Locks
Exlusive Locks
@{
*/

/*! create a recursive? or not exlusive lock*/
extern DSN_API dsn_handle_t dsn_exlock_create(bool recursive);
extern DSN_API void dsn_exlock_destroy(dsn_handle_t l);
extern DSN_API void dsn_exlock_lock(dsn_handle_t l);
extern DSN_API bool dsn_exlock_try_lock(dsn_handle_t l);
extern DSN_API void dsn_exlock_unlock(dsn_handle_t l);
/*@}*/

/*!
@defgroup sync-rwlock Non-recursive Read-Write Locks
Non-recursive Read-Write Locks
@{
*/
extern DSN_API dsn_handle_t dsn_rwlock_nr_create();
extern DSN_API void dsn_rwlock_nr_destroy(dsn_handle_t l);
extern DSN_API void dsn_rwlock_nr_lock_read(dsn_handle_t l);
extern DSN_API void dsn_rwlock_nr_unlock_read(dsn_handle_t l);
extern DSN_API bool dsn_rwlock_nr_try_lock_read(dsn_handle_t l);
extern DSN_API void dsn_rwlock_nr_lock_write(dsn_handle_t l);
extern DSN_API void dsn_rwlock_nr_unlock_write(dsn_handle_t l);
extern DSN_API bool dsn_rwlock_nr_try_lock_write(dsn_handle_t l);
/*@}*/

/*!
@defgroup sync-sema Semaphore
Semaphore
@{
*/
/*! create a semaphore with initial count equals to inital_count */
extern DSN_API dsn_handle_t dsn_semaphore_create(int initial_count);
extern DSN_API void dsn_semaphore_destroy(dsn_handle_t s);
extern DSN_API void dsn_semaphore_signal(dsn_handle_t s, int count);
extern DSN_API void dsn_semaphore_wait(dsn_handle_t s);
extern DSN_API bool dsn_semaphore_wait_timeout(dsn_handle_t s, int timeout_milliseconds);
/*@}*/

/*@}*/

/*@}*/
