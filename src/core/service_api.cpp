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

    /*BOOL WINAPI CtrlHandler(uint32_t fdwCtrlType)
    {
        rdsn::tools::syste_exit.execute(SYS_EXIT_BREAK, 0, nullptr, 0);
        return FALSE;
    }

    static LPTOP_LEVEL_EXCEPTION_FILTER s_originalFilter = nullptr;
    LONG ExecuteOriginalExceptionFilter(syste_exit_type type, int err, void* context)
    {
        if (nullptr != s_originalFilter && context != nullptr)
        {
            return s_originalFilter((struct _EXCEPTION_POINTERS*)context);
        }
        else
        {
            return EXCEPTION_CONTINUE_EXECUTION;
        }
    }

    LONG WINAPI TopLevelExceptionFilter(_In_ struct _EXCEPTION_POINTERS *ExceptionInfo)
    {
        return rdsn::tools::syste_exit.execute(SYS_EXIT_EXCEPTION, 0, (void*)ExceptionInfo, EXCEPTION_CONTINUE_EXECUTION);
    }*/

        
    bool initialize(const char* config_file)
    {
        configuration_ptr config(new configuration(config_file));
        service_spec spec;
        if (!spec.init(config))
        {
            printf ("error in config file %s, exit ...\n", config_file);
            return false;
        }

        // pause when necessary
        if (config->get_value<bool>("core", "pause_on_start", false))
        {
            printf ("\nPause for debugging ...\n");
            getchar();
        }

        // init tools
        s_currentTool = utils::factory_store<tool_app>::create(spec.tool.c_str(), 0, spec.tool.c_str(), config);
        s_currentTool->install(spec);

        // prepare minimum necessary
        service_engine::instance().prepare_minimum_providers_for_toollets(spec);

        // init toollets
        for (auto it = spec.toollets.begin(); it != spec.toollets.end(); it++)
        {
            auto tlet = rdsn::tools::internal_use_only::get_toollet(it->c_str(), 0, config);
            rdsn_assert(tlet, "toolet not found");
            tlet->install(spec);
        }

        // init provider specific system inits
        rdsn::tools::syste_init.execute(config_file);

        //// register system exit callbacks
        //::SetConsoleCtrlHandler(CtrlHandler, TRUE);
        //s_originalFilter = SetUnhandledExceptionFilter(TopLevelExceptionFilter);
        //if (nullptr != s_originalFilter)
        //{
        //    rdsn::tools::syste_exit.put_native(ExecuteOriginalExceptionFilter);
        //}

        // init runtime
        auto err = service_engine::instance().start(spec); 
        if (0 != err)
        {
            printf ("error during rdsn initialization, err = %x", err.get());
            return false;
        }

        // init apps
        for (auto it = spec.app_specs.begin(); it != spec.app_specs.end(); it++)
        {
            if (it->run)
            {
                service_app* app = utils::factory_store<service_app>::create(it->type.c_str(), 0, &(*it), config);
                rdsn_assert(app != nullptr, "Cannot create service app with type name '%s'", it->type.c_str());
                service_apps::instance().add(app);
            }
        }

        s_allApps = &service_apps::instance();

        return true;
    }

    namespace internal_use_only 
    {
        bool register_service(const char* name, service_app_factory factory)
        {
            return utils::factory_store<service_app>::register_factory(name, factory, 0);
        }
    }

    void run()
    { 
        end_point pimaryAddr = service_engine::instance().primary_address();
        auto apps = service_apps::instance().get_all_apps();

        for (auto it = apps.begin(); it != apps.end(); it++)
        {
            service_app* app = it->second;
            auto node = service_engine::instance().start_secondary(app->address().port);
            app->set_service_node(node);
            app->set_address(node->rpc()->address());
        }

        // start the tool
        s_currentTool->run();
    }
}

namespace rpc
{
    const end_point& get_primary_address()
    {
        return service_engine::instance().primary_address();
    }

    const end_point& get_local_address()
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

        return node->rpc()->address();
    }

    bool register_rpc_handler(task_code code, const char* name, rpc_server_handler* handler)
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

        rpc_handler_ptr h(new rpc_handler_info(code));
        h->name = std::string(name);
        h->handler = handler;
        h->service_address = node->rpc()->address();

        return node->rpc()->register_rpc_handler(h);
    }

    bool unregister_rpc_handler(task_code code)
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

        return node->rpc()->unregister_rpc_handler(code);
    }

    rpc_response_task_ptr call(const end_point& server, message_ptr& request, rpc_response_task_ptr callback)
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

        rpc_engine* rpc = node->rpc();

        request->header().from_address = rpc->address();
        request->header().to_address = server;
        rpc->call(request, callback);
        return callback;
    }
    
    void reply(message_ptr& response)
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

        node->rpc()->reply(response);
    }
}

namespace file
{
    handle_t open(const char* file_name, int flag, int pmode)
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

        return node->disk()->open(file_name, flag, pmode);
    }

    void read(handle_t hFile, char* buffer, int count, uint64_t offset, aio_task_ptr& callback)
    {
        auto node = task::get_current_node();
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

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
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

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
        rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

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
