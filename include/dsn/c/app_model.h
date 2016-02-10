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


// rDSN allows many apps in the same process for easy deployment and test
// app ceate, start, and destroy callbacks
typedef void*       (*dsn_app_create)( ///< return app_context,
    const char*     ///< type name registered on dsn_register_app
    );
typedef dsn_error_t(*dsn_app_start)(
    void*,          ///< context return by app_create
    int,            ///< argc
    char**          ///< argv
    );
typedef void(*dsn_app_destroy)(
    void*,          ///< context return by app_create
    bool            ///< cleanup app state or not
    );


typedef struct dsn_app_info
{
    void* app_context_ptr;                    ///< returned by dsn_app_create
                                              ///< See comments in struct service_app_spec about meanings of the following fields.
    int   app_id;                             ///< app id
    int   index;                              ///< app role index
    char  role[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app role name
    char  type[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app type name
    char  name[DSN_MAX_APP_TYPE_NAME_LENGTH]; ///< app full name
    char  data_dir[DSN_MAX_PATH];             ///< app data directory
} dsn_app_info;

typedef struct dsn_app
{
    // default (layer 1, mask = DSN_APP_MASK_DEFAULT)
    char            type_name[DSN_MAX_APP_TYPE_NAME_LENGTH];
    dsn_app_create  create;
    dsn_app_start   start;
    dsn_app_destroy destroy;

    // partitioned service



    // layer 3

} dsn_app;


# define DSN_APP_MASK_DEFAULT 0x0
# define DSN_APP_VNODE        0x1 ///< whether partitioning is supported
# define DSN_APP_REPLICATION  0x2 ///< whether replication is supported
# define DSN_APP_STATEFUL     0x4 ///< whether the app is stateful


//------------------------------------------------------------------------------
//
// system
//
//------------------------------------------------------------------------------
extern DSN_API bool      dsn_register_app(
                            uint32_t app_mask,
                            dsn_app* app_type
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
extern DSN_API int  dsn_get_all_apps(/*out*/ dsn_app_info* info_buffer, int count); // return real app count
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


# ifdef __cplusplus
}
# endif