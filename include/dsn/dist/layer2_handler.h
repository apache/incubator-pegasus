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
 *     interface of layer2 frameworks
 *
 * Revision history:
 *     Feb., 2016, @imzhenyu (Zhenyu Guo), first draft
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# pragma once

# include <dsn/service_api_cpp.h>
# include <dsn/cpp/service_app.h>

 /*
  base contract for the layer2 frameworks on server side

 rDSN has three properties at layer 2 for applications to config, namely

 - stateful or not? whether the application is stateful.

 - partitioned or not? whether the application needs to be partitioned.

 - replicated or not? whether the application needs to be replicated.
  */
namespace dsn
{
    class service_node;
    class app_manager;

    class layer2_handler : public service_app
    {
    public:
        layer2_handler(dsn_gpid gpid) : service_app(gpid) {}
        virtual void on_request(dsn_gpid gpid, bool is_write, dsn_message_t msg) = 0;

    public:
        static void on_layer2_rpc_request(void* app, dsn_gpid gpid, bool is_write, dsn_message_t msg)
        {
            auto sapp = (layer2_handler*)app;
            return sapp->on_request(gpid, is_write, msg);
        }
    };

    /*! C++ wrapper of the \ref dsn_register_app function for layer 2 frameworks */
    template<typename TServiceApp>
    void register_layer2_framework(const char* type_name, uint64_t framework_mask)
    {
        dsn_app app;
        memset(&app, 0, sizeof(app));
        app.mask = framework_mask;
        strncpy(app.type_name, type_name, sizeof(app.type_name));
        app.layer1.create = service_app::app_create<TServiceApp>;
        app.layer1.start = service_app::app_start;
        app.layer1.destroy = service_app::app_destroy;

        app.layer2.frameworks.on_rpc_request = layer2_handler::on_layer2_rpc_request;

        dsn_register_app(&app);
    }
}
