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
# include <dsn/service_api.h>
# include <dsn/tool_api.h>
# include "service_engine.h"
# include "task_engine.h"
# include "rpc_engine.h"
# include "disk_engine.h"
# include <dsn/internal/coredump.h>
# include <dsn/internal/env_provider.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/nfs.h>
# include <boost/filesystem.hpp>
# include "command_manager.h"
# include <thread>

using namespace dsn::tools;

namespace dsn { namespace service {

    static tool_app* s_tool = nullptr;
    static service_apps* s_apps = nullptr;
    static bool s_init = false;

    class system_runner
    {
    public:
        static bool run(const char* config_file, bool sleep_after_init)
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
#if defined(_WIN32)
                printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(::GetCurrentProcessId()));
#else
                printf("\nPause for debugging (pid = %d)...\n", static_cast<int>(getpid()));
#endif
                getchar();
            }

            // setup coredump
            if (!boost::filesystem::exists(spec.coredump_dir))
            {
                boost::filesystem::create_directory(spec.coredump_dir);
            }
            std::string cdir = boost::filesystem::canonical(boost::filesystem::path(spec.coredump_dir)).string();
            utils::coredump::init(cdir.c_str());

            // init tools
            s_tool = utils::factory_store<tool_app>::create(spec.tool.c_str(), 0, spec.tool.c_str(), config);
            s_tool->install(spec);

            // prepare minimum necessary
            service_engine::instance().init_before_toollets(spec);

            // init toollets
            for (auto it = spec.toollets.begin(); it != spec.toollets.end(); it++)
            {
                auto tlet = dsn::tools::internal_use_only::get_toollet(it->c_str(), 0, config);
                dassert(tlet, "toolet not found");
                tlet->install(spec);
            }

            // init provider specific system inits
            dsn::tools::syste_init.execute(config_file);

            // TODO: register syste_exit execution

            // init runtime
            service_engine::instance().init_after_toollets();

            s_init = true;

            // init apps
            for (auto it = spec.app_specs.begin(); it != spec.app_specs.end(); it++)
            {
                if (it->run)
                {
                    service_app* app = utils::factory_store<service_app>::create(it->type.c_str(), 0, &(*it), config);
                    dassert(app != nullptr, "Cannot create service app with type name '%s'", it->type.c_str());
                    service_apps::instance().add(app);
                }
            }

            s_apps = &service_apps::instance();

            auto apps = service_apps::instance().get_all_apps();
            for (auto it = apps.begin(); it != apps.end(); it++)
            {
                service_app* app = it->second;
                auto node = service_engine::instance().start_node(app->spec().id, app->name(), app->spec().ports);
                app->set_service_node(node);
            }

            // start cli if necessary
            if (config->get_value<bool>("core", "cli_local", true))
            {
                ::dsn::command_manager::instance().start_local_cli();
            }

            if (config->get_value<bool>("core", "cli_remote", true))
            {
                ::dsn::command_manager::instance().start_remote_cli();
            }

            // start the tool
            s_tool->run();

            //
            if (sleep_after_init)
            {
                #ifdef max
                #undef max
                #endif
                std::this_thread::sleep_for(std::chrono::hours::max());
            }

            return true;
        }
    };

namespace system
{
    namespace internal_use_only 
    {
        bool register_service(const char* name, service_app_factory factory)
        {
            return utils::factory_store<service_app>::register_factory(name, factory, 0);
        }
    }
    
    bool run(const char* config_file, bool sleep_after_init)
    {
        return ::dsn::service::system_runner::run(config_file, sleep_after_init);
    }

    bool is_ready()
    {
        return s_init;
    }
}

namespace rpc
{
    const end_point& primary_address()
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");
        
        return tsk->node()->rpc()->primary_address();
    }

    bool register_rpc_handler(task_code code, const char* name, rpc_server_handler* handler)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        rpc_handler_ptr h(new rpc_handler_info(code));
        h->name = std::string(name);
        h->handler = handler;

        return tsk->node()->rpc()->register_rpc_handler(h);
    }

    bool unregister_rpc_handler(task_code code)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        return tsk->node()->rpc()->unregister_rpc_handler(code);
    }

    rpc_response_task_ptr call(const end_point& server, message_ptr& request, rpc_response_task_ptr callback)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        if (nullptr == callback)
        {
            callback.reset(new rpc_response_task_empty(request));
        }

        rpc_engine* rpc = tsk->node()->rpc();
        request->header().to_address = server;
        rpc->call(request, callback);
        return callback;
    }

    void call_one_way(const end_point& server, message_ptr& request)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        rpc_response_task_ptr nil;
        rpc_engine* rpc = tsk->node()->rpc();
        request->header().to_address = server;
        rpc->call(request, nil);
    }
    
    void reply(message_ptr& response)
    {
        rpc_engine::reply(response);
    }
}

namespace file
{
    handle_t open(const char* file_name, int flag, int pmode)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        return tsk->node()->disk()->open(file_name, flag, pmode);
    }

    void read(handle_t hFile, char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        callback->aio()->buffer = buffer;
        callback->aio()->buffer_size = count;
        callback->aio()->engine = nullptr;
        callback->aio()->file = hFile;
        callback->aio()->file_offset = offset;
        callback->aio()->type = AIO_Read;

        tsk->node()->disk()->read(callback);
    }

    void write(handle_t hFile, const char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        callback->aio()->buffer = (char*)buffer;
        callback->aio()->buffer_size = count;
        callback->aio()->engine = nullptr;
        callback->aio()->file = hFile;
        callback->aio()->file_offset = offset;
        callback->aio()->type = AIO_Write;

        tsk->node()->disk()->write(callback);
    }

    error_code close(handle_t hFile)
    {
        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        return tsk->node()->disk()->close(hFile);
    }

    void copy_remote_files(
        const end_point& remote,
        std::string& source_dir,
        std::vector<std::string>& files,  // empty for all
        std::string& dest_dir,
        bool overwrite,
        aio_task_ptr& callback
        )
    {
        std::shared_ptr<remote_copy_request> rci(new remote_copy_request());
        rci->source = remote;
        rci->source_dir = source_dir;
        rci->files = files;
        rci->dest_dir = dest_dir;
        rci->overwrite = overwrite;

        auto tsk = task::get_current_task();
        dassert(tsk != nullptr, "this function can only be invoked inside tasks");

        return tsk->node()->nfs()->call(rci, callback);
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

}} // end namespace dsn::service
