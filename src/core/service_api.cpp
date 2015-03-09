/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# include <rdsn/service_api.h>
# include <rdsn/tool_api.h>
# include "service_engine.h"
# include "task_engine.h"
# include "rpc_engine.h"
# include "disk_engine.h"
# include <rdsn/internal/env_provider.h>
# include <rdsn/internal/factory_store.h>

using namespace rdsn::tools;

namespace rdsn { namespace service {

namespace system
{
    static tool_app* s_currentTool = nullptr;
    static service_apps* s_allApps = nullptr;
    
    namespace internal_use_only 
    {
        bool register_service(const char* name, service_app_factory factory)
        {
            return utils::factory_store<service_app>::register_factory(name, factory, 0);
        }
    }
    
    bool run(const char* config_file)
    {
        configuration_ptr config(new configuration(config_file));
        service_spec spec;
        if (!spec.init(config))
        {
            printf("error in config file %s, exit ...\n", config_file);
            return false;
        }

        // pause when necessary
        if (config->get_value<bool>("core", "pause_on_start", false))
        {
            printf("\nPause for debugging ...\n");
            getchar();
        }


        // init tools
        s_currentTool = utils::factory_store<tool_app>::create(spec.tool.c_str(), 0, spec.tool.c_str(), config);
        s_currentTool->install(spec);

        // prepare minimum necessary
        service_engine::instance().init_before_toollets(spec);

        // init toollets
        for (auto it = spec.toollets.begin(); it != spec.toollets.end(); it++)
        {
            auto tlet = rdsn::tools::internal_use_only::get_toollet(it->c_str(), 0, config);
            rassert(tlet, "toolet not found");
            tlet->install(spec);
        }

        // init provider specific system inits
        rdsn::tools::syste_init.execute(config_file);

        // TODO: register syste_exit execution

        // init runtime
        service_engine::instance().init_after_toollets();
        
        // init apps
        for (auto it = spec.app_specs.begin(); it != spec.app_specs.end(); it++)
        {
            if (it->run)
            {
                service_app* app = utils::factory_store<service_app>::create(it->type.c_str(), 0, &(*it), config);
                rassert(app != nullptr, "Cannot create service app with type name '%s'", it->type.c_str());
                service_apps::instance().add(app);
            }
        }

        s_allApps = &service_apps::instance();

        auto apps = service_apps::instance().get_all_apps();
        for (auto it = apps.begin(); it != apps.end(); it++)
        {
            service_app* app = it->second;
            auto node = service_engine::instance().start_node(app->address().port);
            app->set_service_node(node);
            app->set_address(node->rpc()->address());
        }

        // start the tool
        s_currentTool->run();
        return true;
    }
}

namespace rpc
{
    const end_point& get_local_address()
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        return node->rpc()->address();
    }

    bool register_rpc_handler(task_code code, const char* name, rpc_server_handler* handler)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        rpc_handler_ptr h(new rpc_handler_info(code));
        h->name = std::string(name);
        h->handler = handler;
        h->service_address = node->rpc()->address();

        return node->rpc()->register_rpc_handler(h);
    }

    bool unregister_rpc_handler(task_code code)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        return node->rpc()->unregister_rpc_handler(code);
    }

    rpc_response_task_ptr call(const end_point& server, message_ptr& request, rpc_response_task_ptr callback)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        rpc_engine* rpc = node->rpc();
        request->header().to_address = server;
        rpc->call(request, callback);
        return callback;
    }
    
    void reply(message_ptr& response)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        node->rpc()->reply(response);
    }
}

namespace file
{
    handle_t open(const char* file_name, int flag, int pmode)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        return node->disk()->open(file_name, flag, pmode);
    }

    void read(handle_t hFile, char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        callback->aio()->buffer = buffer;
        callback->aio()->buffer_size = count;
        callback->aio()->engine = nullptr;
        callback->aio()->file = hFile;
        callback->aio()->file_offset = offset;
        callback->aio()->type = AIO_Read;

        node->disk()->read(callback);
    }

    void write(handle_t hFile, const char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        callback->aio()->buffer = (char*)buffer;
        callback->aio()->buffer_size = count;
        callback->aio()->engine = nullptr;
        callback->aio()->file = hFile;
        callback->aio()->file_offset = offset;
        callback->aio()->type = AIO_Write;

        node->disk()->write(callback);
    }

    error_code close(handle_t hFile)
    {
        auto node = task::get_current_node();
        rassert(node != nullptr, "this function can only be invoked inside tasks");

        return node->disk()->close(hFile);
    }
}

namespace env
{
    // since Epoch (1970-01-01 00:00:00 +0000 (UTC))
    uint64_t now_ns()
    {
        return service_engine::instance().env()->now_ns();
    }

    // generate random number [min, max]
    uint64_t random64(uint64_t min, uint64_t max)
    {
        return service_engine::instance().env()->random64(min, max);
    }
}

}} // end namespace rdsn::service
