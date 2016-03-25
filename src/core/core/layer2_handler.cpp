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

DSN_API dsn_error_t dsn_layer1_app_create(dsn_gpid gpid, /*our*/ void** app_context)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().create_layer1_app(gpid, app_context);
}

DSN_API dsn_error_t dsn_layer1_app_start(void* app_context)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().start_layer1_app(app_context);
}

DSN_API dsn_error_t dsn_layer1_app_destroy(void* app_context, bool cleanup)
{
    return ::dsn::task::get_current_node2()->get_l2_handler().destroy_layer1_app(app_context, cleanup);
}

DSN_API void dsn_layer1_app_commit_rpc_request(void* app_context, dsn_message_t msg, bool exec_inline)
{
    auto app = (::dsn::layer2_handler_core::layer1_app_info*)(app_context);

    if (exec_inline)
    {
        app->server_dispatcher->on_request_with_inline_execution((::dsn::message_ex*)(msg), ::dsn::task::get_current_node2());
    }
    else
    {
        auto tsk = app->server_dispatcher->on_request((::dsn::message_ex*)(msg), ::dsn::task::get_current_node2());
        if (tsk)
            tsk->enqueue();
        else
        {
            dassert(false, "to be handled");
        }
    }
}

DSN_API dsn_error_t dsn_layer1_app_checkpoint(void* app_context)
{
    auto app = (::dsn::layer2_handler_core::layer1_app_info*)(app_context);
    return app->role->layer2_apps_type_1.chkpt(app->app_context);
}

DSN_API dsn_error_t dsn_layer1_app_checkpoint_async(void* app_context)
{
    auto app = (::dsn::layer2_handler_core::layer1_app_info*)(app_context);
    return app->role->layer2_apps_type_1.chkpt_async(app->app_context);
}

DSN_API int dsn_layer1_app_prepare_learn_request(void* app_context, void* buffer, int capacity)
{
    auto app = (::dsn::layer2_handler_core::layer1_app_info*)(app_context);
    return app->role->layer2_apps_type_1.learn_prepare(app->app_context, buffer, capacity);
}

DSN_API dsn_error_t dsn_layer1_app_get_checkpoint(
    void* app_context,
    int64_t start,
    void*   learn_request,
    int     learn_request_size,
    /* inout */ dsn_app_learn_state* state,
    int state_capacity
    )
{
    auto app = (::dsn::layer2_handler_core::layer1_app_info*)(app_context);
    return app->role->layer2_apps_type_1.chkpt_get(app->app_context, start, learn_request, learn_request_size, state, state_capacity);
}

DSN_API dsn_error_t dsn_layer1_app_apply_checkpoint(void* app_context, const dsn_app_learn_state* state, dsn_chkpt_apply_mode mode)
{
    auto app = (::dsn::layer2_handler_core::layer1_app_info*)(app_context);
    return app->role->layer2_apps_type_1.chkpt_apply(app->app_context, state, mode);
}
