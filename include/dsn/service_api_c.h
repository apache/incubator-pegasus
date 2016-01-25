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
 *     this file define the C Service API in rDSN layer 1, e.g., Zion.
 *
 * ------------------------------------------------------------------------------
 * 
 *  The service system call API for Zion
 * -------------------------------------------
 *  Summary:
 *  (1) rich API for common distributed system development
 *      - thread pools and tasking
 *      - thread synchronization
 *      - remote procedure calls
 *      - asynchnous file operations
 *      - envrionment inputs
 *      - rDSN system and other utilities
 *  (2) portable
 *      - compilable on many platforms (currently linux, windows, FreeBSD, MacOS)
 *      - system calls are in C so that later language wrappers are possibles.
 *  (3) high performance
 *      - all low level components can be plugged with the tool API (in C++)
 *        besides the existing high performance providers;
 *      - developers can also configure thread pools, thread numbers, thread/task
 *        priorities, CPU core affinities, throttling policies etc. declaratively
 *        to build a best threading model for upper apps.
 *  (4) ease of intergration
 *      - support many languages through language wrappers based this c interface
 *      - easy support for existing protocols (thrift/protobuf etc.)
 *      - integrate with existing platform infra with low level providers (plug-in),
 *        such as loggers, performance counters, etc.
 *  (5) rich debug, development tools and runtime policies support
 *      - tool API with task granularity semantic for further tool and runtime policy development.
 *      - rich existing tools, tracer, profiler, simulator, model checker, replayer, global checker
 *  (7) PRINCIPLE: all non-determinims must be go through these system calls so that powerful
 *      internal tools are possible - replay, model checking, replication, ...,
 *      AND, it is still OK to call other DETERMINISTIC APIs for applications.
 * 
 * ------------------------------------------------------------------------------
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version in cpp
 *     July, 2015, @imzhenyu (Zhenyu Guo), refactor and refined in c
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <stdint.h>
# include <stddef.h>
# include <stdarg.h>

# ifdef __cplusplus
# define DEFAULT(value) = value
# define NORETURN [[noreturn]]
# else
# define DEFAULT(value)
# define NORETURN 
# endif

# ifdef __cplusplus
extern "C" {
# else
# include <stdbool.h>
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
# define DSN_MAX_APP_TYPE_NAME_LENGTH      32
# define DSN_MAX_APP_COUNT_IN_SAME_PROCESS 256
# define DSN_MAX_PATH                      1024

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
typedef void*       dsn_group_t;
typedef void*       dsn_uri_t;

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
typedef void        (*dsn_task_cancelled_handler_t)(void*);

// rDSN allows many apps in the same process for easy deployment and test
// app ceate, start, and destroy callbacks
typedef void*       (*dsn_app_create)(          // return app_context,
                                const char*     // type name registered on dsn_register_app_role
                                );              
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

// rDSN allows apps/tools to register commands into its command line interface,
// which can be further probed via local/remote console, and also http services
typedef struct dsn_cli_reply
{
    const char* message;  // zero-ended reply message
    uint64_t size;        // message_size
    void* context;        // context for free_handler
} dsn_cli_reply;

typedef void  (*dsn_cli_handler)(
    void* context,                  // context registered by dsn_cli_register
    int argc,                       // argument count
    const char** argv,              // arguments
    /*out*/dsn_cli_reply* reply     // reply message
    );
typedef void (*dsn_cli_free_handler)(dsn_cli_reply reply);

typedef struct dsn_app_info
{
    void* app_context_ptr;                    // returned by dsn_app_create
    // See comments in struct service_app_spec about meanings of the following fields.
    int   app_id;                             // app id
    int   index;                              // app role index
    char  role[DSN_MAX_APP_TYPE_NAME_LENGTH]; // app role name
    char  type[DSN_MAX_APP_TYPE_NAME_LENGTH]; // app type name
    char  name[DSN_MAX_APP_TYPE_NAME_LENGTH]; // app full name
    char  data_dir[DSN_MAX_PATH];             // app data directory
} dsn_app_info;

// the following ctrl code are used by dsn_file_ctrl
typedef enum dsn_ctrl_code_t
{
    CTL_BATCH_INVALID,
    CTL_BATCH_WRITE,            // (batch) set write batch size
    CTL_MAX_CON_READ_OP_COUNT,  // (throttling) maximum concurrent read ops
    CTL_MAX_CON_WRITE_OP_COUNT, // (throttling) maximum concurrent write ops
} dsn_ctrl_code_t;

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
    HOST_TYPE_INVALID = 0,
    HOST_TYPE_IPV4 = 1,  // 4 bytes
    HOST_TYPE_GROUP = 2, // reference to an address group
    HOST_TYPE_URI = 3,   // universal resource identifier    
    HOST_TYPE_COUNT = 4    
} dsn_host_type_t;

typedef struct dsn_address_t
{
    union u_t {
        struct {
            unsigned long long type : 2;
            unsigned long long padding : 14;
            unsigned long long port : 16;
            unsigned long long ip : 32;
        } v4;
        struct {
            unsigned long long type : 2;
            unsigned long long uri : 62;   // dsn_uri_t
        } uri;
        struct {
            unsigned long long type : 2;
            unsigned long long group : 62; // dsn_group_t
        } group;
        uint64_t value;
    } u;
} dsn_address_t;

typedef union dsn_msg_context_t
{
    struct {
        uint64_t write_replication : 1;
        uint64_t read_replication : 1;
        uint64_t read_semantic : 2;
        uint64_t unused : 10;
        uint64_t parameter : 50; // parameter for the flags, e.g., snapshort decree for replication read
    } u;
    uint64_t context;
} dsn_msg_context_t;

# define DSN_MSGM_TIMEOUT (0x1 << 0)
# define DSN_MSGM_HASH    (0x1 << 1)
# define DSN_MSGM_VNID    (0x1 << 2)
# define DSN_MSGM_CONTEXT (0x1 << 3)

typedef struct dsn_msg_options_t
{
    int               timeout_ms;  
    int               thread_hash; 
    uint64_t          vnid;
    dsn_msg_context_t context;
} dsn_msg_options_t;

//------------------------------------------------------------------------------
//
// system
//
//------------------------------------------------------------------------------
extern DSN_API bool      dsn_register_app_role(
                            const char* type_name, 
                            dsn_app_create create, 
                            dsn_app_start start, 
                            dsn_app_destroy destroy
                            );
extern DSN_API void      dsn_register_app_checker(
                            const char* name, 
                            dsn_checker_create create, 
                            dsn_checker_apply apply
                            );
extern DSN_API bool      dsn_mimic_app(
                            const char* app_name, // specified in config file as [apps.${app_name}]
                            int index // start from 1, when there are multiple instances
                            );
extern DSN_API bool      dsn_run_config(
                            const char* config, 
                            bool sleep_after_init DEFAULT(false)
                            );
//
// run the system with arguments
//   config [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]
// e.g., config.ini -app_list replica@1 to start the first replica as a new process
//       config.ini -app_list replica to start ALL replicas (count specified in config) as a new process
//       config.ini -app_list replica -cargs replica-port=34556 to start ALL replicas
//                 with given port variable specified in config.ini
//       config.ini to start ALL apps as a new process
//
// Note the argc, argv folllows the C main convention that argv[0] is the executable name
//
extern DSN_API void dsn_run(int argc, char** argv, bool sleep_after_init DEFAULT(false));
NORETURN extern DSN_API void dsn_exit(int code);
extern DSN_API int  dsn_get_all_apps(dsn_app_info* info_buffer, int count); // return real app count
extern DSN_API bool dsn_get_current_app_info(/*out*/ dsn_app_info* app_info);
extern DSN_API const char* dsn_get_current_app_data_dir();

//
// app roles must be registered (dsn_app_register_role)
// before dsn_run is invoked.
// in certain cases, a synchonization is needed to ensure this order.
// for example, we want to register an app role in python while the main program is in 
// C++ to call dsn_run.
// in this case, we need to do as follows (in C++)
//    [ C++ program    
//    start new thread[]{
//       [ python program
//           dsn_app_register_role(...)
//           dsn_app_loader_signal()
//       ]
//    };
//
//    dsn_app_loader_wait();
//    dsn_run(...)
//    ]
//
extern DSN_API void dsn_app_loader_signal();
extern DSN_API void dsn_app_loader_wait();

extern DSN_API const char* dsn_cli_run(const char* command_line); // return command output
extern DSN_API void        dsn_cli_free(const char* command_output);

// return value: Handle (the handle of this registered command)
extern DSN_API dsn_handle_t dsn_cli_register(
                            const char* command,
                            const char* help_one_line,
                            const char* help_long,
                            void* context,
                            dsn_cli_handler cmd_handler,
                            dsn_cli_free_handler output_freer
                            );
// return value: Handle (the handle of this registered command)
// or NULL (We did no find a service_node, probably you call this function from a non-rdsn thread)
extern DSN_API dsn_handle_t dsn_cli_app_register(
                            const char* command,   // auto-augmented by rDSN as $app_full_name.$command
                            const char* help_one_line,
                            const char* help_long,
                            void* context,         // context to be forwareded to cmd_handler
                            dsn_cli_handler cmd_handler,
                            dsn_cli_free_handler output_freer
                            );

// remove a cli handler, parameter: return value of dsn_cli_register or dsn_cli_app_register
extern DSN_API void dsn_cli_deregister(dsn_handle_t cli_handle);



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
extern DSN_API bool                  dsn_task_current(dsn_task_t t); // is inside given task
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
extern DSN_API void                  dsn_config_dump(const char* file);

// logs with level smaller than this start_level will not be logged
extern DSN_API dsn_log_level_t       dsn_log_start_level;
extern DSN_API dsn_log_level_t       dsn_log_get_start_level();
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

extern DSN_API uint64_t               dsn_crc64_compute(const void* ptr, size_t size, uint64_t init_crc);

//
// Given
//      x_final = dsn_crc64_compute (x_ptr, x_size, x_init);
// and
//      y_final = dsn_crc64_compute (y_ptr, y_size, y_init);
// compute CRC of concatenation of A and B
//      x##y_crc = dsn_crc64_compute (x##y, x_size + y_size, xy_init);
// without touching A and B
//

extern DSN_API uint64_t              dsn_crc64_concatenate(
                                        uint32_t xy_init,
                                        uint64_t x_init,
                                        uint64_t x_final,
                                        size_t x_size,
                                        uint64_t y_init,
                                        uint64_t y_final,
                                        size_t y_size);

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
extern DSN_API uint32_t      dsn_ipv4_from_host(const char* name);

extern DSN_API uint32_t      dsn_ipv4_local(const char* network_interface);

extern DSN_API dsn_address_t dsn_address_build(
                                const char* host, 
                                uint16_t port
                                );
extern DSN_API dsn_address_t dsn_address_build_ipv4(
                                uint32_t ipv4,
                                uint16_t port
                                );
extern DSN_API dsn_address_t dsn_address_build_group(
                                dsn_group_t g
                                );
extern DSN_API dsn_address_t dsn_address_build_uri(
                                dsn_uri_t uri
                                );

extern DSN_API const char*   dsn_address_to_string(dsn_address_t addr);

extern DSN_API dsn_uri_t     dsn_uri_build(const char* url); // must be paired with destroy later
extern DSN_API void          dsn_uri_destroy(dsn_uri_t uri);

extern DSN_API dsn_group_t   dsn_group_build(const char* name); // must be paired with release later
extern DSN_API bool          dsn_group_add(dsn_group_t g, dsn_address_t ep);
extern DSN_API bool          dsn_group_remove(dsn_group_t g, dsn_address_t ep);
extern DSN_API void          dsn_group_set_leader(dsn_group_t g, dsn_address_t ep);
extern DSN_API dsn_address_t dsn_group_get_leader(dsn_group_t g);
extern DSN_API bool          dsn_group_is_leader(dsn_group_t g, dsn_address_t ep);
extern DSN_API dsn_address_t dsn_group_next(dsn_group_t g, dsn_address_t ep);
extern DSN_API dsn_address_t dsn_group_forward_leader(dsn_group_t g);
extern DSN_API void          dsn_group_destroy(dsn_group_t g);

extern DSN_API dsn_address_t dsn_primary_address();

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
                                int timeout_milliseconds DEFAULT(0),
                                int hash DEFAULT(0)
                                );
extern DSN_API dsn_message_t dsn_msg_create_response(dsn_message_t request);
extern DSN_API dsn_message_t dsn_msg_copy(dsn_message_t msg);
extern DSN_API void          dsn_msg_add_ref(dsn_message_t msg);
extern DSN_API void          dsn_msg_release_ref(dsn_message_t msg);
extern DSN_API void          dsn_msg_set_options(
                                dsn_message_t msg,
                                dsn_msg_options_t *opts,
                                uint32_t mask // set opt bits using DSN_MSGM_XXX
                                );

extern DSN_API void         dsn_msg_get_options(
                                dsn_message_t msg,
                                /*out*/ dsn_msg_options_t* opts
                                );

// apps write rpc message as follows:
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

// apps read rpc message as follows:
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
extern DSN_API dsn_address_t dsn_msg_from_address(dsn_message_t msg);
extern DSN_API dsn_address_t dsn_msg_to_address(dsn_message_t msg);
extern DSN_API uint64_t      dsn_msg_rpc_id(dsn_message_t msg);

//
// server-side rpc calls
//
extern DSN_API bool          dsn_rpc_register_handler(
                                dsn_task_code_t code, 
                                const char* name,
                                dsn_rpc_request_handler_t cb, 
                                void* context
                                );

// return void* context on dsn_rpc_register_handler  
extern DSN_API void*         dsn_rpc_unregiser_handler(
                                dsn_task_code_t code
                                );

// reply with a response which is created using dsn_msg_create_response
extern DSN_API void          dsn_rpc_reply(dsn_message_t response);

// forward the request to another server instead
extern DSN_API void          dsn_rpc_forward(dsn_message_t request, dsn_address_t addr);


//
// client-side rpc calls
//
// create a callback task to be used in dsn_rpc_call (@rpc-call)
extern DSN_API dsn_task_t    dsn_rpc_create_response_task(
                                dsn_message_t request, 
                                dsn_rpc_response_handler_t cb, 
                                void* context, 
                                int reply_hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );
extern DSN_API dsn_task_t    dsn_rpc_create_response_task_ex(
                                dsn_message_t request, 
                                dsn_rpc_response_handler_t cb, 
                                dsn_task_cancelled_handler_t on_cancel,
                                void* context, 
                                int reply_hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );

// tracker can be empty
extern DSN_API void          dsn_rpc_call(
                                dsn_address_t server,
                                dsn_task_t rpc_call
                                );

// WARNING: returned msg must be explicitly msg_release_ref
extern DSN_API dsn_message_t dsn_rpc_call_wait(
                                dsn_address_t server, 
                                dsn_message_t request
                                );
extern DSN_API void          dsn_rpc_call_one_way(
                                dsn_address_t server, 
                                dsn_message_t request
                                );

// WARNING: returned msg must be explicitly msg_release_ref
extern DSN_API dsn_message_t dsn_rpc_get_response(dsn_task_t rpc_call);

// this is to mimic a response is received when no real rpc is called
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

typedef struct
{
    void* buffer;
    int size;
}dsn_file_buffer_t;

// return nullptr if open failed
extern DSN_API dsn_handle_t dsn_file_open(
                                const char* file_name, 
                                int flag, 
                                int pmode
                                );
extern DSN_API dsn_error_t  dsn_file_close(
                                dsn_handle_t file
                                );
extern DSN_API dsn_error_t  dsn_file_flush(
                                dsn_handle_t file
                                );
// native handle: HANDLE for windows, int for non-windows
extern DSN_API void*        dsn_file_native_handle(dsn_handle_t file);
extern DSN_API dsn_task_t   dsn_file_create_aio_task(
                                dsn_task_code_t code, 
                                dsn_aio_handler_t cb, 
                                void* context,
                                int hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );
extern DSN_API dsn_task_t   dsn_file_create_aio_task_ex(
                                dsn_task_code_t code, 
                                dsn_aio_handler_t cb, 
                                dsn_task_cancelled_handler_t on_cancel,
                                void* context,
                                int hash DEFAULT(0),
                                dsn_task_tracker_t tracker DEFAULT(nullptr)
                                );
extern DSN_API void         dsn_file_read(
                                dsn_handle_t file, 
                                char* buffer, 
                                int count, 
                                uint64_t offset, 
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_write(
                                dsn_handle_t file, 
                                const char* buffer, 
                                int count, 
                                uint64_t offset, 
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_write_vector(
                                dsn_handle_t file,
                                const dsn_file_buffer_t* buffers,
                                int buffer_count,
                                uint64_t offset,
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_copy_remote_directory(
                                dsn_address_t remote, 
                                const char* source_dir, 
                                const char* dest_dir,
                                bool overwrite, 
                                dsn_task_t cb
                                );
extern DSN_API void         dsn_file_copy_remote_files(
                                dsn_address_t remote,
                                const char* source_dir, 
                                const char** source_files, 
                                const char* dest_dir, 
                                bool overwrite, 
                                dsn_task_t cb
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

__inline uint64_t dsn_now_us() { return dsn_now_ns() / 1000; }
__inline uint64_t dsn_now_ms() { return dsn_now_ns() / 1000000; }

__inline uint32_t dsn_random32(uint32_t min, uint32_t max)
{
    return (uint32_t)(dsn_random64(min, max)); 
}

__inline double   dsn_probability()
{
    return (double)(dsn_random64(0, 1000000000)) / 1000000000.0; 
}

//------------------------------------------------------------------------------
//
// service model (partition + replication)
//
//------------------------------------------------------------------------------

typedef struct __app_flag__
{
    uint32_t is_partitioned   : 1;
    uint32_t is_stateful      : 1;
    uint32_t is_replicated    : 1;
    uint32_t use_virtual_node : 1;
} dsn_app_flag;

typedef struct __app_id__
{
    uint32_t app_id;
    uint32_t partition_id;
} dsn_app_id;

typedef struct __app_role__
{
    const char*     type_name;    
    dsn_app_flag    flag;
    dsn_app_create  create;
    dsn_app_start   start;
    dsn_app_destroy destroy;

    // dsn_app_checkpointer
    // dsn_app_load_balancer
    // dsn_app_xxx
} dsn_app_role;

//------------------------------------------------------------------------------
//
// perf counters
//
//------------------------------------------------------------------------------

typedef enum dsn_perf_counter_type_t
{
    COUNTER_TYPE_NUMBER,
    COUNTER_TYPE_RATE,
    COUNTER_TYPE_NUMBER_PERCENTILES,
    COUNTER_TYPE_INVALID,
    COUNTER_TYPE_COUNT
} dsn_perf_counter_type_t;

typedef enum dsn_perf_counter_percentile_type_t
{
    COUNTER_PERCENTILE_50,
    COUNTER_PERCENTILE_90,
    COUNTER_PERCENTILE_95,
    COUNTER_PERCENTILE_99,
    COUNTER_PERCENTILE_999,

    COUNTER_PERCENTILE_COUNT,
    COUNTER_PERCENTILE_INVALID
} dsn_perf_counter_percentile_type_t;

extern DSN_API dsn_handle_t dsn_perf_counter_create(const char* section, const char* name, dsn_perf_counter_type_t type, const char* description);
extern DSN_API void dsn_perf_counter_remove(dsn_handle_t handle);
extern DSN_API void dsn_perf_counter_increment(dsn_handle_t handle);
extern DSN_API void dsn_perf_counter_decrement(dsn_handle_t handle);
extern DSN_API void dsn_perf_counter_add(dsn_handle_t handle, uint64_t val);
extern DSN_API void dsn_perf_counter_set(dsn_handle_t handle, uint64_t val);
extern DSN_API double dsn_perf_counter_get_value(dsn_handle_t handle);
extern DSN_API uint64_t dsn_perf_counter_get_integer_value(dsn_handle_t handle);
extern DSN_API double dsn_perf_counter_get_percentile(dsn_handle_t handle, dsn_perf_counter_percentile_type_t type);
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
            dlog(LOG_LEVEL_FATAL, __FILE__, "assertion expression: "#x); \
            dlog(LOG_LEVEL_FATAL, __FILE__, __VA_ARGS__);  \
            dsn_coredump();       \
                } } while (false)

#ifndef NDEBUG
#define dbg_dassert dassert
#else
#define dbg_dassert(x, ...) 
#endif

# ifdef __cplusplus
}

inline void dsn_address_size_checker()
{
    static_assert (sizeof(dsn_address_t) == sizeof(uint64_t),
        "sizeof(dsn_address_t) must equal to sizeof(uint64_t)");

    static_assert (sizeof(dsn_msg_context_t) == sizeof(uint64_t),
        "sizeof(dsn_msg_context_t) must equal to sizeof(uint64_t)");
}
# endif
