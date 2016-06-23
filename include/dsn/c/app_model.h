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
 *     application model in rDSN
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/c/api_common.h>

# ifdef __cplusplus
namespace dsn { class service_app; }
extern "C" {
# endif


/*!
@defgroup dev-layer1-models Overview
@ingroup dev-layer1
*/

/*!
  @defgroup app-model Application Model
  @ingroup dev-layer1-models

  Application and deployment model for rDSN applications.

  - Developers define the required application models and register them into rDSN 
  so that the latter can manage the applications appropriately, such as creat/destroy/scale-out/replicate them.

  <PRE>
  int main(int argc, char** argv)
  {
    // register all app types
    dsn::register_app<test_client>("test");
    dsn::register_app<test_server>("server");

    // run rDSN
    dsn_run(argc, argv, true);
    return 0;
  }
  </PRE>

  - Developers config the application instances in config files, and rDSN creates
  them accordingly on start-up.

  <PRE>

  [apps..default]
  ; arguments for the app instances
  arguments =

  ; count of app instances for this type (ports are 
  ; automatically calculated accordingly to avoid confliction)
  count = 1

  ; delay seconds for when the apps should be started
  delay_seconds = 0

  ; path of a dynamic library which implement this app role, and register itself upon loaded
  dmodule =

  ;
  ; when the service cannot automatically register its app types into rdsn
  ; through %dmoudule%'s dllmain or attribute(constructor), we require the %dmodule%
  ; implement an exporte function called "dsn_error_t dsn_bridge(const char* args);",
  ; which loads the real target (e.g., a python/Java/php module), that registers their
  ; app types and factories.
  dmodule_bridge_arguments =

  ; thread pools need to be started
  pools =

  ; RPC server listening ports needed for this app
  ports =

  ; whether to run the app instances or not
  run = true

  ; app type name, as given when registering by dsn_register_app
  type =
  
  [apps.client]
  arguments = localhost 20101
  
  delay_seconds = 1

  pools = THREAD_POOL_DEFAULT, THREAD_POOL_TEST_TASK_QUEUE_1

  type = test
  
  [apps.server]
  
  pools = THREAD_POOL_DEFAULT, THREAD_POOL_TEST_SERVER

  ports = 20101
  
  type = test

  </PRE>

  - Developers config the main tools and toollets to run the process, among
    many other configurations.

  <PRE>
  [core]
  ; use what tool to run this process, e.g., native, simulator, or fastrun
  tool = fastrun

  ; use what toollets, e.g., tracer, profiler, fault_injector
  toollets = tracer, profiler, fault_injector

  ; aio aspect providers, usually for tooling purpose
  aio_aspects =

  ; asynchonous file system provider
  aio_factory_name =

  ; whether to enable local command line interface (cli)
  cli_local = true

  ; whether to enable remote command line interface (using dsn.cli)
  cli_remote = false

  ; where to put the all the data/log/coredump, etc..
  data_dir = ./data

  ; how many disk engines? IOE_PER_NODE, or IOE_PER_QUEUE
  disk_io_mode =

  ; whether to start a default service app for serving the rDSN calls made in
  ; non-rDSN threads, so that developers do not need to write dsn_mimic_app call before them
  ; in this case, a [apps.mimic] section must be defined in config files
  enable_default_app_mimic = false

  ; environment aspect providers, usually for tooling purpose
  env_aspects =

  ; environment provider
  env_factory_name =

  ; io thread count, only for IOE_PER_NODE; for IOE_PER_QUEUE, task workers are served as io threads
  io_worker_count = 1

  ; recursive lock aspect providers, usually for tooling purpose
  lock_aspects =

  ; recursive exclusive lock provider
  lock_factory_name =

  ; non-recurisve lock aspect providers, usually for tooling purpose
  lock_nr_aspects =

  ; non-recurisve exclusive lock provider
  lock_nr_factory_name =

  ; logging provider
  logging_factory_name = dsn::tools::simple_logger

  ; logs with level below this will not be logged
  logging_start_level = LOG_LEVEL_DEBUG

  ; network aspect providers, usually for tooling purpose
  network_aspects =

  ; nfs provider
  nfs_factory_name =

  ; how many nfs engines? IOE_PER_NODE, or IOE_PER_QUEUE
  nfs_io_mode =

  ; whether to pause at startup time for easier debugging
  pause_on_start = false

  ; peformance counter provider
  perf_counter_factory_name =

  ; maximum number of performance counters
  perf_counter_max_count = 10000

  ; how many rpc engines? IOE_PER_NODE, or IOE_PER_QUEUE
  rpc_io_mode =

  ; non-recursive rwlock aspect providers, usually for tooling purpose
  rwlock_nr_aspects =

  ; non-recurisve rwlock provider
  rwlock_nr_factory_name =

  ; semaphore aspect providers, usually for tooling purpose
  semaphore_aspects =

  ; semaphore provider
  semaphore_factory_name =

  ; whether to start nfs
  start_nfs = false

  ; timer service aspect providers, usually for tooling purpose
  timer_aspects =

  ; timer service provider
  timer_factory_name =

  ; how many disk timer services? IOE_PER_NODE, or IOE_PER_QUEUE
  timer_io_mode =

  ; thread number for timer service for core itself
  timer_service_worker_count = 1
  </PRE>

  - Developers can also optionally configure many others to fit their
    special requirement according to the application and the scenario.
    For full configurations, developers can set ```[core] cli_local = true```,
    and run ```config-dump``` command to get the latest config file with
    the help information.

  @{
 */
 
/*! callback to create the app context */
typedef void*       (*dsn_app_create)(
    const char*,    ///< type name registered on dsn_register_app
    dsn_gpid        ///< assigned global partition id
    );

/*! callback to run the app with the app context, similar to main(argc, argv) */
typedef dsn_error_t(*dsn_app_start)(
    void*,          ///< context return by app_create
    int,            ///< argc
    char**          ///< argv
    );

/*! callback to stop and destroy the app */
typedef dsn_error_t(*dsn_app_destroy)(
    void*,          ///< context return by app_create
    bool            ///< cleanup app state or not
    );

/*! callback for layer2 app & framework to handle incoming rpc request */
typedef void(*dsn_framework_rpc_request_handler)(
    void*,          ///< context from dsn_app_create
    dsn_gpid,       ///< global partition id
    bool,           ///< is_write_operation or not
    dsn_message_t   ///< incoming rpc request
    );

struct dsn_app_learn_state
{
    int     total_learn_state_size; // memory used in the given buffer by this learn-state 
    int64_t from_decree_excluded;
    int64_t to_decree_included;
    int     meta_state_size;
    int     file_state_count;
    void*   meta_state_ptr;
    const char** files;
};

enum dsn_chkpt_apply_mode
{
    DSN_CHKPT_COPY,
    DSN_CHKPT_LEARN
};

typedef void(*dsn_app_on_batched_write_requests)(
    void*,           ///< context from dsn_app_create
    int64_t,         ///< decree
    dsn_message_t*,  ///< request array ptr
    int              ///< request count
    );

typedef int(*dsn_app_get_physical_error)(
    void*     ///< context from dsn_app_create
    );

typedef dsn_error_t(*dsn_app_sync_checkpoint)(
    void*,    ///< context from dsn_app_create
    int64_t   ///< current last committed decree
    );

typedef dsn_error_t(*dsn_app_async_checkpoint)(
    void*,    ///< context from dsn_app_create
    int64_t   ///< current last committed decree
    );

typedef int64_t(*dsn_app_get_last_checkpoint_decree)(
    void*     ///< context from dsn_app_create
    );

typedef dsn_error_t(*dsn_app_prepare_get_checkpoint)(
    void*,    ///< context from dsn_app_create
    void*,    ///< buffer for filling in learn request
    int,      ///< buffer capacity
    int*      ///< occupied size
    );

typedef dsn_error_t(*dsn_app_get_checkpoint)(
    void*,    ///< context from dsn_app_create
    int64_t,  ///< learn start decree
    int64_t,  ///< local last committed decree
    void*,    ///< learn request from prepare_get_checkpoint
    int,      ///< learn request size
    dsn_app_learn_state*, ///< learn state buffer to be filled in
    int                   ///< learn state buffer capacity
    );

typedef dsn_error_t(*dsn_app_apply_checkpoint)(
    void*,                     ///< context from dsn_app_create
    dsn_chkpt_apply_mode,      ///< checkpoint apply mode
    int64_t,                   ///< local last committed decree
    const dsn_app_learn_state* ///< learn state
    );

# define DSN_APP_MASK_APP        0x01 ///< app mask
# define DSN_APP_MASK_FRAMEWORK  0x02 ///< framework mask

# pragma pack(push, 4)

/*!
  developers define the following dsn_app data structure, and passes it
  to rDSN through \ref dsn_register_app so that the latter can manage 
  the app appropriately.
 */
typedef union dsn_app_callbacks
{
    dsn_app_create placeholder[DSN_MAX_CALLBAC_COUNT];
    struct app_callbacks
    {
        dsn_app_on_batched_write_requests   on_batched_write_requests;
        dsn_app_get_physical_error          get_physical_error;
        dsn_app_sync_checkpoint             sync_checkpoint;
        dsn_app_async_checkpoint            async_checkpoint;
        dsn_app_get_last_checkpoint_decree  get_last_checkpoint_decree;
        dsn_app_prepare_get_checkpoint      prepare_get_checkpoint;
        dsn_app_get_checkpoint              get_checkpoint;
        dsn_app_apply_checkpoint            apply_checkpoint;
    } calls;    
} dsn_app_callbacks;

typedef struct dsn_app
{
    uint64_t        mask; ///< application capability mask
    char            type_name[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< type 

    /*! layer 1 app definition, mask = DSN_APP_MASK_APP */
    struct layer1_callbacks
    {
        dsn_app_create  create;  ///< callback to create the context for the app
        dsn_app_start   start;   ///< callback to start the app, similar to ```main```
        dsn_app_destroy destroy; ///< callback to stop and destroy the app
    } layer1;    

    struct
    {
        /*! framework model */
        struct layer2_framework_callbacks
        {
            dsn_framework_rpc_request_handler on_rpc_request;
        } frameworks;

        /*! app model (for integration with frameworks) */
        dsn_app_callbacks apps;
    } layer2;    
} dsn_app;
# pragma pack(pop)

# pragma pack(push, 4)
/*! application information retrived at runtime */
typedef struct dsn_app_info
{
    //
    // layer 1 information
    //
    union {
        void* app_context_ptr;                ///< returned by dsn_app_create
# ifdef __cplusplus
        ::dsn::service_app *app_ptr_cpp;
# endif
    } app;
    
    int   app_id;                             ///< app id, see \ref service_app_spec for more details.
    int   index;                              ///< app role index
    char  role[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app role name
    char  type[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app type name
    char  name[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app full name
    char  data_dir[DSN_MAX_PATH];             ///< app data directory
    dsn_address_t primary_address;            ///< primary address
} dsn_app_info;
# pragma pack(pop)

/*!
 register application/framework into rDSN runtime
 
 \param app_type requried app type information.

 \return true if it succeeds, false if it fails.

 An example is as follows:
 <PRE>
     dsn_app app;
     memset(&app, 0, sizeof(app));
     app.mask = DSN_APP_MASK_APP;
     strncpy(app.type_name, type_name, sizeof(app.type_name));
     app.layer1.create = service_app::app_create<TServiceApp>;
     app.layer1.start = service_app::app_start;
     app.layer1.destroy = service_app::app_destroy;

     dsn_register_app(&app);
 </PRE>
 */
extern DSN_API bool      dsn_register_app(dsn_app* app_type);

/*!
 get application callbacks registered into rDSN runtime
 
 \param name app type name

 \param callbacks  output callbacks

 \return true it it exists, false otherwise
 */
extern DSN_API bool      dsn_get_app_callbacks(const char* name, /* out */ dsn_app_callbacks* callbacks);

/*!
 mimic an app as if the following execution in the current thread are
 executed in the target app's threads. 

 \param app_name name of the application, note it is not the type name
 \param index    one-based index of the application instances

 \return true if it succeeds, false if it fails.

 This is useful when we want to leverage 3rd party library into rDSN
 application and call rDSN service API in the threads that are created
 by the 3rd party code. 

 For cases we simply want to use a rDSN-based client library in a non-rDSN
 application, developers can simply set [core] enable_default_app_mimic = true
 in configuration file. See more details at \ref enable_default_app_mimic.

 */
extern DSN_API bool      dsn_mimic_app(
                            const char* app_name, ///< specified in config file as [apps.${app_name}]
                            int index ///< start from 1, when there are multiple instances
                            );

/*!
 start the system with given configuration

 \param config           the configuration file for this run
 \param sleep_after_init whether to sleep after rDSN initialization, default is false

 \return true if it succeeds, false if it fails.
 */
extern DSN_API bool      dsn_run_config(
                            const char* config, 
                            bool sleep_after_init DEFAULT(false)
                            );

/*!
 start the system with given arguments

 \param argc             argc in C main convention
 \param argv             argv in C main convention
 \param sleep_after_init whether to sleep after rDSN initialization, default is false

 \return true if it succeeds, false if it fails.
  
 Usage:
   config-file [-cargs k1=v1;k2=v2] [-app_list app_name1@index1;app_name2@index]

 Examples:
 - config.ini -app_list replica@1 to start the first replica as a new process
 - config.ini -app_list replica to start ALL replicas (count specified in config) as a new
 process
 - config.ini -app_list replica -cargs replica-port=34556 to start ALL replicas
   with given port variable specified in config.ini
 - config.ini to start ALL apps as a new process

 Note the argc, argv folllows the C main convention that argv[0] is the executable name.
 */
extern DSN_API void dsn_run(int argc, char** argv, bool sleep_after_init DEFAULT(false));

/*!
 exit the process with the given exit code

 \param code exit code for the process

 rDSN runtime does not provide elegant exit routines. Thereafter, developers call dsn_exit
 to exit the current process to avoid exceptions happending during normal exit.
 */
NORETURN extern DSN_API void dsn_exit(int code);

/*!
 get rDSN application (instance)s information in the current process

 \param info_buffer buffer for storing information data.
 \param count       capacity of the buffer

 \return how many rDSN application( instance)s are running in the current processs.

 The returned value may be larger than count - in this casse, developers need to allocate
 a new buffer that is enough to hold the information of returned number of applications.
 */
extern DSN_API int  dsn_get_all_apps(/*out*/ dsn_app_info* info_buffer, int count);

/*!
 get current rDSN application information.

 \param app_info buffer for storing information data.

 \return true if it succeeds, false if the current thread does not belong to any rDSN app.
 */
extern DSN_API bool dsn_get_current_app_info(/*out*/ dsn_app_info* app_info);

extern DSN_API dsn_app_info* dsn_get_app_info_ptr(dsn_gpid gpid DEFAULT(dsn_gpid{ 0 }));

/*!
 get current application data dir.

 \return null if it fails, else a pointer to the data path string.
 */
extern DSN_API const char* dsn_get_app_data_dir(dsn_gpid gpid DEFAULT(dsn_gpid{ 0 }));

/*!
 signal the application loader that application types are registered.

 in rDSN, app types must be registered via \ref dsn_app_register.
 before \ref dsn_run is invoked. in certain cases, a synchonization is needed to ensure this order.
 for example, we want to register an app role in python while the main program is in C++ to
 call dsn_run. in this case, we need to do as follows (in C++)

 <PRE>
    new thread([]{
       [ python program
           dsn_app_register(...)
           dsn_app_loader_signal()
       ]
    });
 
    dsn_app_loader_wait();
    dsn_run(...)
    ].
 </PRE>
 */
extern DSN_API void dsn_app_loader_signal();

/*! wait signal from \ref dsn_app_loader_signal. */
extern DSN_API void dsn_app_loader_wait();

/*@}*/


# ifdef __cplusplus
}
# endif
