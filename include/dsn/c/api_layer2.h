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
 *     layer 2 (eon) API  in rDSN
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/c/api_layer1.h>

# ifdef __cplusplus
extern "C" {
# endif

/*!
 @defgroup framework-api Framework APIs
 @ingroup service-api-c
    
  APIs used by frameworks
  
 @{
 */
 
/*!
Creates framework hosted application.

\param type        registered app type
\param gpid        assigned gpid.
\param data_idr    assigned data directory
\param app_context_for_downcalls app_context for usage by dsn_hosted_app_create/start/destroy
\param app_context_for_callbacks  app_context used by upcalls retrived from dsn_get_app_callbacks

\return error code: ERR_OK, ERR_SERVICE_ALREADY_EXIST (app_context is also valid)
*/
extern DSN_API dsn_error_t dsn_hosted_app_create(
    const char* type,
    dsn_gpid gpid, 
    const char* data_dir,
    /*our*/ void** app_context_for_downcalls, 
    /*out*/void** app_context_for_callbacks
    );

/*!
start framework hosted application.

\param app_context_for_downcalls see \ref dsn_hosted_app_create
\param argc same convention with traditional main
\param argv same convention with traditional main
*/
extern DSN_API dsn_error_t dsn_hosted_app_start(void* app_context_for_downcalls, int argc, char** argv);

/*!
destroy framework hosted application.

\param app_context_for_downcalls see \ref dsn_hosted_app_create
\param cleanup clean up the state of given application
*/
extern DSN_API dsn_error_t dsn_hosted_app_destroy(void* app_context_for_downcalls, bool cleanup);

/*!
send an RPC request to a local application and execute

\param app_context_for_downcalls see \ref dsn_hosted_app_create
\param msg the RPC request
\param exec_inline whether to execute the RPC handler within this call or not
*/
extern DSN_API void        dsn_hosted_app_commit_rpc_request(void* app_context_for_downcalls, dsn_message_t msg, bool exec_inline);

/*@}*/

# ifdef __cplusplus
}
# endif