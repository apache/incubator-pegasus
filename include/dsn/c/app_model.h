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

#pragma once

#include <dsn/c/api_common.h>

#ifdef __cplusplus
namespace dsn {
class service_app;
}
extern "C" {
#endif

/*!
  @defgroup service-api-model Application and Framework Models
  @ingroup service-api-c

  The base interface (models) for applications and frameworks atop rDSN.
  In rDSN, both applications and frameworks must implement a base abstract
  called \ref dsn_app, which are registered into rDSN's service
  kernel via \ref dsn_register_app.

  Here is an example where we register two applications into rDSN; note we
  use the C++ wrappers atop our C API in this example.

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

  After the applications and frameworks are registered, developers specify
  the concrete instances in config files, and rDSN creates them accordingly on start-up.

  <PRE>
  [apps.client]
  arguments = localhost 20101
  delay_seconds = 1
  pools = THREAD_POOL_DEFAULT
  type = test

  [apps.server]
  pools = THREAD_POOL_TEST_SERVER
  ports = 20101
  type = server
  </PRE>

  Developers usually run this using ```./app config.ini```, or ```./app``` for more options.
  @{
 */

/*!
    callback to create the app context

    \param app_name type name registered on dsn_register_app
    \param id       assigned global partition id

    \return         the app context used by other APIs to reference this application instance
 */
typedef void *(*dsn_app_create)(const char *app_name, dsn_gpid id);

/*!
    callback to run the app with the app context, similar to main(argc, argv)

    \param app   context returned by dsn_app_create
    \param argc  as in traditional main(argc, argv)
    \param argv  as in traditional main(argc, argv)

    \return error code for app start
 */
typedef dsn_error_t (*dsn_app_start)(void *app, int argc, char **argv);

/*!
    callback to stop and destroy the app

    \param app   context returned by dsn_app_create
    \param cleanup whether to cleanup the state belonging to this app

    \return error code for app destroy
 */
typedef dsn_error_t (*dsn_app_destroy)(void *app, bool cleanup);

/*!
    callback for framework to handle incoming rpc request, implemented by frameworks

    \param app   context returned by dsn_app_create
    \param gpid  global partition id
    \param is_write_operation whether the incoming rpc reqeust is a write operation or not
    \param request incoming rpc request message
 */
typedef void (*data_engine_interceptor)(void *app,
                                        dsn_gpid gpid,
                                        bool is_write_operation,
                                        dsn_message_t request);

/*!
developers define the following dsn_app data structure, and passes it
to rDSN through \ref dsn_register_app so that the latter can manage
the app appropriately.

Click into the corresponding types for what are the callback means.
*/
#pragma pack(push, 4)
typedef struct dsn_app
{
    char type_name[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< type

    dsn_app_create create;   ///< callback to create the context for the app
    dsn_app_start start;     ///< callback to start the app, similar to ```main```
    dsn_app_destroy destroy; ///< callback to stop and destroy the app
    data_engine_interceptor intercepted_request; ///< callback to data engine's request
} dsn_app;
#pragma pack(pop)

#pragma pack(push, 4)
/*! application information retrived at runtime */
typedef struct dsn_app_info
{
    //
    // app information
    //
    union
    {
        void *app_context_ptr; ///< returned by dsn_app_create
#ifdef __cplusplus
        ::dsn::service_app *app_ptr_cpp;
#endif
    } app;

    int app_id; ///< app id, see \ref service_app_spec for more details.
    int index;  ///< app role index
    char role[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app role name
    char type[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app type name
    char name[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app full name
    char data_dir[DSN_MAX_PATH];             ///< app data directory
    dsn_address_t primary_address;           ///< primary address
} dsn_app_info;
#pragma pack(pop)

/*!
 register application/framework into rDSN runtime

 \param app_type requried app type information.

 \return true if it succeeds, false if it fails.

 An example is as follows:
 <PRE>
     dsn_app app;
     memset(&app, 0, sizeof(app));
     strncpy(app.type_name, type_name, sizeof(app.type_name));
     app.create = service_app::app_create<TServiceApp>;
     app.start = service_app::app_start;
     app.destroy = service_app::app_destroy;
     app.interceted_request = service_app::on_intercepted_request;
     dsn_register_app(&app);
 </PRE>
 */
extern DSN_API bool dsn_register_app(dsn_app *app_type);

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
extern DSN_API bool
dsn_mimic_app(const char *app_name, ///< specified in config file as [apps.${app_name}]
              int index             ///< start from 1, when there are multiple instances
              );

/*!
 start the system with given configuration

 \param config           the configuration file for this run
 \param sleep_after_init whether to sleep after rDSN initialization, default is false

 \return true if it succeeds, false if it fails.
 */
extern DSN_API bool dsn_run_config(const char *config, bool sleep_after_init DEFAULT(false));

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
extern DSN_API void dsn_run(int argc, char **argv, bool sleep_after_init DEFAULT(false));

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
extern DSN_API int dsn_get_all_apps(/*out*/ dsn_app_info *info_buffer, int count);

/*!
 get current rDSN application information.

 \param app_info buffer for storing information data.

 \return true if it succeeds, false if the current thread does not belong to any rDSN app.
 */
extern DSN_API bool dsn_get_current_app_info(/*out*/ dsn_app_info *app_info);

extern DSN_API dsn_app_info *dsn_get_app_info_ptr(dsn_gpid gpid DEFAULT(dsn_gpid{0}));

/*!
 get current application data dir.

 \return null if it fails, else a pointer to the data path string.
 */
extern DSN_API const char *dsn_get_app_data_dir(dsn_gpid gpid DEFAULT(dsn_gpid{0}));

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

#ifdef __cplusplus
}
#endif
