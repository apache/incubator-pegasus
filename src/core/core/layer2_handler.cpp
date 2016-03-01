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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
 
# include <dsn/service_api_c.h>
# include <dsn/internal/task.h>
# include "service_engine.h"
# include "rpc_engine.h"


DSN_API dsn_error_t dsn_create_layer1_app(dsn_gpid gpid, /*our*/ void** app_context)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().create_layer1_app(gpid, app_context);
}

DSN_API dsn_error_t dsn_start_layer1_app(void* app_context, int argc, char** argv)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().start_layer1_app(app_context, argc, argv);
}

DSN_API void dsn_destroy_layer1_app(void* app_context, bool cleanup)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().destroy_layer1_app(app_context, cleanup);
}

DSN_API void dsn_handle_layer1_rpc_request(void* app_context, dsn_message_t msg)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().commit_layer1(app_context, msg);
}
