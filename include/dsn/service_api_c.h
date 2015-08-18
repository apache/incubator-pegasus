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
 *
 * History
 *  - major revision from C++ in July, 2015 by @imzhenyu (Zhenyu.Guo@microsoft.com)
 */
# pragma once

# include <stdint.h>
# include <stddef.h>
# include <stdarg.h>

# ifdef __cplusplus
extern "C" {
# endif

# if defined(DSN_IN_CORE)
    # if defined(_WIN32)
    # define DSN_API __declspec(dllexport)
    # else
    # define DSN_API __attribute__((visibility("default")))
    # endif
# else
    # if defined(_WIN32)
    # define DSN_API __declspec(dllimport)
    # else
    # define DSN_API
    # endif
# endif

# define DSN_MAX_TASK_CODE_NAME_LENGTH     48
# define DSN_MAX_ADDRESS_NAME_LENGTH       48
# define DSN_MAX_BUFFER_COUNT_IN_MESSAGE   64
# define DSN_INVALID_HASH                  0xdeadbeef
# define DSN_MAX_APP_TYPE_NAME_LENGTH      32
# define DSN_MAX_APP_COUNT_IN_SAME_PROCESS 256

//------------------------------------------------------------------------------
//
// The service system call API for rDSN
//-------------------------------------------
// Summary:
// (1) rich API for common distributed system development
//     - thread pools and tasking
//     - thread synchronization
//     - remote procedure calls
//     - asynchnous file operations
//     - envrionment inputs 
//     - rDSN system and other utilities
// (2) portable
//     - compilable on many platforms (currently linux, windows, FreeBSD, MacOS)
//     - system calls are in C so that later language wrappers are possibles.
// (3) high performance
//     - all low level components can be plugged with the tool API (in C++)
//       besides the existing high performance providers; 
//     - developers can also configure thread pools, thread numbers, thread/task 
//       priorities, CPU core affinities, throttling policies etc. declaratively
//       to build a best threading model for upper apps.
// (4) ease of intergration
//     - support many languages through language wrappers based this c interface
//     - easy support for existing protocols (thrift/protobuf etc.)
//     - integrate with existing platform infra with low level providers (plug-in),
//       such as loggers, performance counters, etc.
// (5) rich debug, development tools and runtime policies support
//     - tool API with task granularity semantic for further tool and runtime policy development.
//     - rich existing tools, tracer, profiler, simulator, model checker, replayer, global checker
// (7) PRINCIPLE: all non-determinims must be go through these system calls so that powerful
//     internal tools are possible - replay, model checking, replication, ...,
//     AND, it is still OK to call other DETERMINISTIC APIs for applications.
//
//------------------------------------------------------------------------------


//------------------------------------------------------------------------------
//
// common data structures
//
//------------------------------------------------------------------------------

// handles and codes
struct dsn_app_info;
typedef struct      dsn_app_info dsn_app_info; // rDSN app information
typedef int         dsn_error_t;
typedef int         dsn_task_code_t;
typedef int         dsn_threadpool_code_t;
typedef void*       dsn_handle_t;
typedef void*       dsn_task_t;
typedef void*       dsn_task_tracker_t;
typedef void*       dsn_message_t; 

// all computation in rDSN are as tasks or events, 
// i.e., in event-driven programming
// all kinds of task callbacks, see dsn_task_type_t below
typedef void        (*dsn_task_handler_t)(void*); // void* context
typedef void        (*dsn_rpc_request_handler_t)(
                                dsn_message_t,  // incoming request
                                void*           // handler context registered
                                );
typedef void        (*dsn_rpc_response_handler_t)(
                                dsn_error_t,    // usually, it is ok, or timeout, or busy
                                dsn_message_t,  // sent rpc request
                                dsn_message_t,  // incoming rpc response
                                void*           // context when rpc is called
                                );
typedef void        (*dsn_aio_handler_t)(
                                dsn_error_t,    //
                                size_t,         // transferred io size
                                void*           // context when rd/wt is called
                                );

// rDSN allows many apps in the same process for easy deployment and test
// app ceate, start, and destroy callbacks
typedef void*       (*dsn_app_create)();        // return app_context
typedef dsn_error_t (*dsn_app_start)(
                                void*,          // context return by app_create
                                int,            // argc
                                char**          // argv
                                );
typedef void        (*dsn_app_destroy)(
                                void*,          // context return by app_create
                                bool            // cleanup app state or not
                                );

// rDSN allows global assert across many apps in the same process
// the global assertions are called checkers
typedef void*       (*dsn_checker_create)(      // return a checker
                                const char*,    // checker name
                                dsn_app_info*,  // apps available to the checker
                                int             // apps count
                                );
typedef void        (*dsn_checker_apply)(void*); // run the given checker

struct dsn_app_info
{
    void* app_context_ptr;                    // returned by dsn_app_create
    int   app_id;                             // assigned by rDSN automatically
    char  type[DSN_MAX_APP_TYPE_NAME_LENGTH]; // see dsn_register_app_role
    char  name[DSN_MAX_APP_TYPE_NAME_LENGTH]; // app name configed in config file
};

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

typedef enum dsn_log_level_t
{
    LOG_LEVEL_INFORMATION,
    LOG_LEVEL_DEBUG,
    LOG_LEVEL_WARNING,
    LOG_LEVEL_ERROR,
    LOG_LEVEL_FATAL,
    LOG_LEVEL_COUNT,
    LOG_LEVEL_INVALID
} dsn_log_level_t;


typedef enum dsn_host_type_t
{
    HOST_TYPE_IPV4,  // 4 bytes
    HOST_TYPE_IPV6,  // 16 bytes
    HOST_TYPE_URL,   // customized bytes
    HOST_TYPE_COUNT,
    HOST_TYPE_INVALID
} dsn_host_type_t;

//
// TODO: support other host types
//
typedef struct dsn_address_t
{
    /*dsn_host_type_t type;
    union {
        uint32_t   ip;
        uint32_t   ipv6[4];
        struct {
            const char *url;
            void (*deletor)(const char*);
        };
    };*/

    uint32_t ip;
    uint16_t port;
    char     name[DSN_MAX_ADDRESS_NAME_LENGTH]; // for verbose debugging
} dsn_address_t;

//------------------------------------------------------------------------------
//
// system
//
//------------------------------------------------------------------------------
extern DSN_API bool dsn_register_app_role(
                        const char* type_name, 
                        dsn_app_create create, 
                        dsn_app_start start, 
                        dsn_app_destroy destroy
                        );
extern DSN_API void dsn_register_app_checker(
                        const char* name, 
                        dsn_checker_create create, 
                        dsn_checker_apply apply
                        );
extern DSN_API bool dsn_run_config(
                        const char* config, 
                        bool sleep_after_init
                        );
//
// run the system with arguments
//   config [-cargs k1=v1;k2=v2] [-app app_name] [-app_index index]
// e.g., config.ini -app replica -app_index 1 to start the first replica as a new process
//       config.ini -app replica to start ALL replicas (count specified in config) as a new process
//       config.ini -app replica -cargs replica-port=34556 to start ALL replicas
//                 with given port variable specified in config.ini
//       config.ini to start ALL apps as a new process
//
// Note the argc, argv folllows the C main convention that argv[0] is the executable name
//
extern DSN_API void dsn_run(int argc, char** argv, bool sleep_after_init);
extern DSN_API int  dsn_get_all_apps(dsn_app_info* info_buffer, int count); // return real app count

//------------------------------------------------------------------------------
//
// common utilities
//
//------------------------------------------------------------------------------
extern DSN_API dsn_error_t           dsn_error_register(const char* name);
extern DSN_API const char*           dsn_error_to_string(dsn_error_t err);    
extern DSN_API dsn_threadpool_code_t dsn_threadpool_code_register(const char* name);
extern DSN_API const char*           dsn_threadpool_code_to_string(dsn_threadpool_code_t pool_code);
extern DSN_API dsn_threadpool_code_t dsn_threadpool_code_from_string(
                                        const char* s, 
                                        dsn_threadpool_code_t default_code // when s is not registered
                                        );
extern DSN_API int                   dsn_threadpool_code_max();
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
extern DSN_API const char*           dsn_config_get_value_string(
                                        const char* section,       // [section]
                                        const char* key,           // key = value
                                        const char* default_value, // if [section] key is not present
                                        const char* dsptr          // what it is for, as help-info in config
                                        );
extern DSN_API bool                  dsn_config_get_value_bool(
                                        const char* section, 
                                        const char* key, 
                                        bool default_value, 
                                        const char* dsptr
                                        );
extern DSN_API uint64_t              dsn_config_get_value_uint64(
                                        const char* section, 
                                        const char* key, 
                                        uint64_t default_value, 
                                        const char* dsptr
                                        );
extern DSN_API double                dsn_config_get_value_double(
                                        const char* section, 
                                        const char* key, 
                                        double default_value, 
                                        const char* dsptr
                                        );
// return all key count (may greater than buffer_count)
extern DSN_API int                   dsn_config_get_all_keys(
                                        const char* section, 
                                        const char** buffers, 
                                        /*inout*/ int* buffer_count
                                        );
// logs with level smaller than this start_level will not be logged
extern DSN_API dsn_log_level_t       dsn_log_start_level;
extern DSN_API void                  dsn_logv(
                                        const char *file, 
                                        const char *function, 
                                        const int line, 
                                        dsn_log_level_t log_level, 
                                        const char* title, 
                                        const char* fmt, 
                                        va_list args
                                        );
extern DSN_API void                  dsn_logf(
                                        const char *file, 
                                        const char *function, 
                                        const int line, 
                                        dsn_log_level_t log_level, 
                                        const char* title, 
                                        const char* fmt, 
                                        ...
                                        );
extern DSN_API void                  dsn_log(
                                        const char *file, 
                                        const char *function, 
                                        const int line, 
                                        dsn_log_level_t log_level,
                                        const char* title
                                        );
extern DSN_API void                  dsn_coredump();
extern DSN_API uint32_t              dsn_crc32_compute(const void* ptr, size_t size, uint32_t init_crc);

//
// Given
//      x_final = dsn_crc32_compute (x_ptr, x_size, x_init);
// and
//      y_final = dsn_crc32_compute (y_ptr, y_size, y_init);
// compute CRC of concatenation of A and B
//      x##y_crc = dsn_crc32_compute (x##y, x_size + y_size, xy_init);
// without touching A and B
//
extern DSN_API uint32_t              dsn_crc32_concatenate(
                                        uint32_t xy_init, 
                                        uint32_t x_init,
                                        uint32_t x_final, 
                                        size_t   x_size, 
                                        uint32_t y_init, 
                                        uint32_t y_final, 
                                        size_t   y_size
                                        );

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

// create a common asynchronous task
// - code defines the thread pool which executes the callback
//   i.e., [task.%code$] pool_code = THREAD_POOL_DEFAULT
// - hash defines the thread with index hash % worker_count in the threadpool
//   to execute the callback, when [threadpool.%pool_code%] partitioned = true
//   
extern DSN_API dsn_task_t  dsn_task_create(
                            dsn_task_code_t code,  // task label
                            dsn_task_handler_t cb, // callback function
                            void* param,           // param to the callback
                            int hash               // hash value to callback
                            );
extern DSN_API dsn_task_t  dsn_task_create_timer(
                            dsn_task_code_t code, 
                            dsn_task_handler_t cb, 
                            void* param, 
                            int hash, 
                            int interval_milliseconds // timer period
                            );
// repeated declarations later in correpondent rpc and file sections
//extern DSN_API dsn_task_t  dsn_rpc_create_response_task(...);
//extern DSN_API dsn_task_t  dsn_file_create_aio_task(...);

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

//
// common task 
// - task: must be created by dsn_task_create or dsn_task_create_timer
// - tracker: can be null. 
//
extern DSN_API void        dsn_task_call(
                                dsn_task_t task, 
                                dsn_task_tracker_t tracker, 
                                int delay_milliseconds
                                );
extern DSN_API bool        dsn_task_cancel(dsn_task_t task, bool wait_until_finished);
extern DSN_API bool        dsn_task_cancel2(
                                dsn_task_t task, 
                                bool wait_until_finished, 
                                /*out*/ bool* finished
                                );
extern DSN_API bool        dsn_task_wait(dsn_task_t task); 
extern DSN_API bool        dsn_task_wait_timeout(
                                dsn_task_t task,
                                int timeout_milliseconds
                                );
extern DSN_API dsn_error_t dsn_task_error(dsn_task_t task);

//------------------------------------------------------------------------------
//
// thread synchronization
//
//------------------------------------------------------------------------------
extern DSN_API dsn_handle_t dsn_exlock_create(bool recursive);
extern DSN_API void         dsn_exlock_destroy(dsn_handle_t l);
extern DSN_API void         dsn_exlock_lock(dsn_handle_t l);
extern DSN_API bool         dsn_exlock_try_lock(dsn_handle_t l);
extern DSN_API void         dsn_exlock_unlock(dsn_handle_t l);

// non-recursive rwlock
extern DSN_API dsn_handle_t dsn_rwlock_nr_create();
extern DSN_API void         dsn_rwlock_nr_destroy(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_lock_read(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_unlock_read(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_lock_write(dsn_handle_t l);
extern DSN_API void         dsn_rwlock_nr_unlock_write(dsn_handle_t l);

extern DSN_API dsn_handle_t dsn_semaphore_create(int initial_count);
extern DSN_API void         dsn_semaphore_destroy(dsn_handle_t s);
extern DSN_API void         dsn_semaphore_signal(dsn_handle_t s, int count);
extern DSN_API void         dsn_semaphore_wait(dsn_handle_t s);
extern DSN_API bool         dsn_semaphore_wait_timeout(
                                dsn_handle_t s, 
                                int timeout_milliseconds
                                );

//------------------------------------------------------------------------------
//
// rpc
//
//------------------------------------------------------------------------------

// rpc address utilities
extern DSN_API dsn_address_t dsn_address_invalid;
extern DSN_API void          dsn_address_build(
                                /*out*/ dsn_address_t* ep, 
                                const char* host, 
                                uint16_t port
                                );
extern DSN_API dsn_address_t dsn_primary_address();
extern DSN_API void          dsn_address_get_invalid(/*out*/ dsn_address_t* paddr);
extern DSN_API void          dsn_primary_address2(/*out*/ dsn_address_t* paddr);
    
// rpc message and buffer management
//
// all returned dsn_message_t are NOT add_ref by rDSN, 
// so you do not need to call msg_release_ref to release the msgs.
// the decision is made for easier programming, and you may consider the later
// dsn_rpc_xxx calls do the resource gc work for you.
// however, if you want to hold the message further after call dsn_rpc_xxx,
// you need to call dsn_msg_add_ref first before these operations, 
// and call dsn_msg_release_ref later to ensure there is no memory leak.
// This is very similar to what we have above with task handles.
//
// however, this is not true for returned message from
// dsn_rpc_call_wait and dsn_rpc_get_response. For these two cases,
// developers are responsible for releasing the message handle
// by calling dsn_msg_release_ref.
//
// similarily, for all msgs accessable in callbacks, if you want to hold them in 
// upper apps further beyond the callbacks, 
// you need to call msg_add_ref, and msg_release_ref explicitly.
//
// when timeout_milliseconds == 0,  [task.%rpc_code%] rpc_timeout_milliseconds is used.
// 
extern DSN_API dsn_message_t dsn_msg_create_request(
                                dsn_task_code_t rpc_code, 
                                int timeout_milliseconds, // if 0, see comments
                                int hash
                                );
extern DSN_API dsn_message_t dsn_msg_create_response(dsn_message_t request);
extern DSN_API void          dsn_msg_add_ref(dsn_message_t msg);
extern DSN_API void          dsn_msg_release_ref(dsn_message_t msg);
extern DSN_API void          dsn_msg_update_request(
                                dsn_message_t msg, 
                                int timeout_milliseconds,  // if == 0, no update
                                int hash // if == DSN_INVALID_HASH, no update
                                );
extern DSN_API void          dsn_msg_query_request(
                                dsn_message_t msg, 
                                /*out*/ int* ptimeout_milliseconds,
                                /*out*/ int* phash
                                );

// apps write rpc message as this:
//   void* ptr;
//   size_t size;
//   dsn_msg_write_next(msg, &ptr, &size, min_size);
//   write msg content to [ptr, ptr + size)
//   dsn_msg_write_commit(msg, real_written_size);
//
// allocate a buffer for message write
// - *ptr returns the writable memory pointer
// - *size returns the writable memory buffer size,
//   which can be larger than min_size
extern DSN_API void          dsn_msg_write_next(
                                dsn_message_t msg, 
                                /*out*/ void** ptr, 
                                /*out*/ size_t* size, 
                                size_t min_size
                                );
// commit the write buffer after the message content is written
extern DSN_API void          dsn_msg_write_commit(dsn_message_t msg, size_t size);

// apps read rpc message as this:
//   void* ptr;
//   size_t size;
//   dsn_msg_read_next(msg, &ptr, &size);
//   read msg content in [ptr, ptr + size)
//   dsn_msg_read_commit(msg, real read size);
// if not committed, next dsn_msg_read_next returns the same read buffer
extern DSN_API bool          dsn_msg_read_next(
                                dsn_message_t msg, 
                                /*out*/ void** ptr, 
                                /*out*/ size_t* size
                                );
extern DSN_API void          dsn_msg_read_commit(dsn_message_t msg, size_t size);

extern DSN_API size_t        dsn_msg_body_size(dsn_message_t msg);
extern DSN_API void*         dsn_msg_rw_ptr(dsn_message_t msg, size_t offset_begin);
extern DSN_API void          dsn_msg_from_address(
                                dsn_message_t msg, 
                                /*out*/ dsn_address_t* ep
                                );
extern DSN_API void          dsn_msg_to_address(
                                dsn_message_t msg, 
                                /*out*/ dsn_address_t* ep
                                );
    
// rpc calls
extern DSN_API bool          dsn_rpc_register_handler(
                                dsn_task_code_t code, 
                                const char* name,
                                dsn_rpc_request_handler_t cb, 
                                void* param
                                );
// return void* param on dsn_rpc_register_handler  
extern DSN_API void*         dsn_rpc_unregiser_handler(dsn_task_code_t code);

extern DSN_API dsn_task_t    dsn_rpc_create_response_task(
                                dsn_message_t request, 
                                dsn_rpc_response_handler_t cb, 
                                void* param, 
                                int reply_hash
                                );
extern DSN_API void          dsn_rpc_call(
                                dsn_address_t server, 
                                dsn_task_t rpc_call, 
                                dsn_task_tracker_t tracker
                                );

// WARNING: returned msg must be explicitly msg_release_ref
extern DSN_API dsn_message_t dsn_rpc_call_wait(dsn_address_t server, dsn_message_t request);
extern DSN_API void          dsn_rpc_call_one_way(dsn_address_t server, dsn_message_t request);
extern DSN_API void          dsn_rpc_reply(dsn_message_t response);

// WARNING: returned msg must be explicitly msg_release_ref
extern DSN_API dsn_message_t dsn_rpc_get_response(dsn_task_t rpc_call);
extern DSN_API void          dsn_rpc_enqueue_response(
                                dsn_task_t rpc_call, 
                                dsn_error_t err, 
                                dsn_message_t response
                                );

//------------------------------------------------------------------------------
//
// file operations
//
//------------------------------------------------------------------------------
extern DSN_API dsn_handle_t dsn_file_open(const char* file_name, int flag, int pmode);
extern DSN_API dsn_error_t  dsn_file_close(dsn_handle_t file);
extern DSN_API dsn_task_t   dsn_file_create_aio_task(
                                dsn_task_code_t code, 
                                dsn_aio_handler_t cb, 
                                void* param, 
                                int hash
                                );
extern DSN_API void         dsn_file_read(
                                dsn_handle_t file, 
                                char* buffer, 
                                int count, 
                                uint64_t offset, 
                                dsn_task_t cb, 
                                dsn_task_tracker_t tracker
                                );
extern DSN_API void         dsn_file_write(
                                dsn_handle_t file, 
                                const char* buffer, 
                                int count, 
                                uint64_t offset, 
                                dsn_task_t cb, 
                                dsn_task_tracker_t tracker
                                );
extern DSN_API void         dsn_file_copy_remote_directory(
                                dsn_address_t remote, 
                                const char* source_dir, 
                                const char* dest_dir,
                                bool overwrite, 
                                dsn_task_t cb, 
                                dsn_task_tracker_t tracker
                                );
extern DSN_API void         dsn_file_copy_remote_files(
                                dsn_address_t remote, 
                                const char* source_dir, 
                                const char** source_files, 
                                const char* dest_dir, 
                                bool overwrite, 
                                dsn_task_t cb, 
                                dsn_task_tracker_t tracker
                                );
extern DSN_API size_t       dsn_file_get_io_size(dsn_task_t cb_task);
extern DSN_API void         dsn_file_task_enqueue(
                                dsn_task_t cb_task, 
                                dsn_error_t err, 
                                size_t size
                                );

//------------------------------------------------------------------------------
//
// environment inputs
//
//------------------------------------------------------------------------------
extern DSN_API uint64_t dsn_now_ns();
extern DSN_API uint64_t dsn_random64(uint64_t min, uint64_t max); // [min, max]

inline uint64_t dsn_now_us() { return dsn_now_ns() / 1000; }
inline uint64_t dsn_now_ms() { return dsn_now_ns() / 1000000; }

inline uint32_t dsn_random32(uint32_t min, uint32_t max) 
{
    return static_cast<uint32_t>(dsn_random64(min, max)); 
}

inline double   dsn_probability() 
{
    return static_cast<double>(dsn_random64(0, 1000000000)) / 1000000000.0; 
}

//------------------------------------------------------------------------------
//
// common marocs
//
//------------------------------------------------------------------------------

#define dlog(level, title, ...) do {if (level >= dsn_log_start_level) \
        dsn_logf(__FILE__, __FUNCTION__, __LINE__, level, title, __VA_ARGS__); } while(false)
#define dinfo(...)  dlog(LOG_LEVEL_INFORMATION, __TITLE__, __VA_ARGS__)
#define ddebug(...) dlog(LOG_LEVEL_DEBUG, __TITLE__, __VA_ARGS__)
#define dwarn(...)  dlog(LOG_LEVEL_WARNING, __TITLE__, __VA_ARGS__)
#define derror(...) dlog(LOG_LEVEL_ERROR, __TITLE__, __VA_ARGS__)
#define dfatal(...) dlog(LOG_LEVEL_FATAL, __TITLE__, __VA_ARGS__)
#define dassert(x, ...) do { if (!(x)) {                    \
            dlog(LOG_LEVEL_FATAL, "assert", #x);           \
            dlog(LOG_LEVEL_FATAL, "assert", __VA_ARGS__);  \
            dsn_coredump();       \
                } } while (false)

#ifdef _DEBUG
#define dbg_dassert dassert
#else
#define dbg_dassert(x, ...) 
#endif

        
# ifdef __cplusplus
}
# endif
