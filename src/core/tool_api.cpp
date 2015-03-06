# include <rdsn/tool_api.h>
# include <rdsn/internal/service_app.h>
# include "service_engine.h"
# include <rdsn/internal/factory_store.h>
# include <rdsn/internal/singleton_store.h>

namespace rdsn { namespace tools {

tool_base::tool_base(const char* name, configuration_ptr c)
{
    _configuration = c;
    _name = name;
}

toollet::toollet(const char* name, configuration_ptr c)
    : tool_base(name, c)
{    
}

tool_app::tool_app(const char* name, configuration_ptr c)
    : tool_base(name, c)
{    
}

DEFINE_TASK_CODE(LPC_CONTROL_SERVICE_APP, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)

class service_control_task : public task
{
public:
    service_control_task(service::service_app* app, bool start)
        : _app(app), task(LPC_CONTROL_SERVICE_APP), _start(start)
    {    
    }

    void exec()
    {
        if (_start)
        {
            auto err = _app->start(_app->arg_count(), _app->args());
            rassert(err == 0, "start app failed, err = %s", err.to_string());
        }
        else
            _app->stop();
    }

private:
    service::service_app* _app;
    bool     _start; // false for stop
};

void tool_app::start_all_service_apps()
{
    auto apps = service::service_apps::instance().get_all_apps();
    for (auto it = apps.begin(); it != apps.end(); it++)
    {
        task_ptr t(new service_control_task(it->second, true));
        t->enqueue(1000 * it->second->spec().delay_seconds, it->second);
    }
}


void tool_app::stop_all_service_apps()
{
    auto apps = service::service_apps::instance().get_all_apps();
    for (auto it = apps.begin(); it != apps.end(); it++)
    {
        task_ptr t(new service_control_task(it->second, false));
        t->enqueue(0, it->second);
    }
}

const service_spec& tool_app::get_service_spec()
{
    return service_engine::instance().spec();
}

configuration_ptr tool_app::config()
{
    return service_engine::instance().spec().config;
}

join_point<void, const char*> syste_init("system.init");
join_point<long, syste_exit_type, int, void*> syste_exit("system.exit"); // type, error code, context (e.g., exception)

namespace internal_use_only
{
    bool register_toollet(const char* name, toollet_factory f, int type)
    {
        return rdsn::utils::factory_store<toollet>::register_factory(name, f, type);
    }

    bool register_tool(const char* name, tool_app_factory f, int type)
    {
        return rdsn::utils::factory_store<tool_app>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, task_queue_factory f, int type)
    {
        return rdsn::utils::factory_store<task_queue>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, task_worker_factory f, int type)
    {
        return rdsn::utils::factory_store<task_worker>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, admission_controller_factory f, int type)
    {
        return rdsn::utils::factory_store<admission_controller>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, lock_factory f, int type)
    {
        return rdsn::utils::factory_store<lock_provider>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, read_write_lock_factory f, int type)
    {
        return rdsn::utils::factory_store<rwlock_provider>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, semaphore_factory f, int type)
    {
        return rdsn::utils::factory_store<semaphore_provider>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, network_factory f, int type)
    {
        return rdsn::utils::factory_store<network>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, aio_factory f, int type)
    {
        return rdsn::utils::factory_store<aio_provider>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, env_factory f, int type)
    {
        return rdsn::utils::factory_store<env_provider>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, perf_counter_factory f, int type)
    {
        return rdsn::utils::factory_store<perf_counter>::register_factory(name, f, type);
    }

    bool register_component_provider(const char* name, logging_factory f, int type)
    {
        return rdsn::utils::factory_store<logging_provider>::register_factory(name, f, type);
    }
    
    toollet* get_toollet(const char* name, int type, configuration_ptr config)
    {
        toollet* tlt = nullptr;
        if (utils::singleton_store<std::string, toollet*>::instance().get(name, tlt))
            return tlt;
        else
        {
            tlt = utils::factory_store<toollet>::create(name, type, name, config);
            utils::singleton_store<std::string, toollet*>::instance().put(name, tlt);
            return tlt;
        }
    }
}
}} // end namespace rdsn::tool_api
