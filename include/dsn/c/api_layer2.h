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
 @defgroup dev-layer2-c C API for layer 2

 @ingroup dev-layer2
    
  layer2 API for building frameworks and applications
  
 @{
 */
 
/*!
Creates layer 1 application.

\param gpid        the gpid.
\param app_context output context for the application.

\return error code: ERR_OK, ERR_SERVICE_ALREADY_EXIST (app_context is also valid)
*/
extern DSN_API dsn_error_t dsn_layer1_app_create(dsn_gpid gpid, /*our*/ void** app_context);

extern DSN_API dsn_error_t dsn_layer1_app_start(void* app_context);

extern DSN_API dsn_error_t dsn_layer1_app_destroy(void* app_context, bool cleanup);

extern DSN_API void        dsn_layer1_app_commit_rpc_request(void* app_context, dsn_message_t msg, bool exec_inline);

extern DSN_API dsn_error_t dsn_layer1_app_checkpoint(void* app_context, int64_t version);

extern DSN_API dsn_error_t dsn_layer1_app_checkpoint_async(void* app_context, int64_t version);

extern DSN_API dsn_error_t dsn_layer1_app_checkpoint_get_version(void* app_context);

extern DSN_API int         dsn_layer1_app_prepare_learn_request(void* app_context, void* buffer, int capacity);

extern DSN_API dsn_error_t dsn_layer1_app_get_checkpoint(
                                void* app_context,
                                int64_t start,
                                int64_t local_commit,
                                void*   learn_request,
                                int     learn_request_size,
                                /* inout */ dsn_app_learn_state* state,
                                int state_capacity
                                );

extern DSN_API dsn_error_t dsn_layer1_app_apply_checkpoint(void* app_context, int64_t local_commit, const dsn_app_learn_state* state, dsn_chkpt_apply_mode mode);

extern DSN_API int         dsn_layer1_app_get_physical_error(void* app_context);

/*@}*/

# ifdef __cplusplus
}
# endif