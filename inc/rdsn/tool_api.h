# pragma once

// providers
# include <rdsn/internal/task_queue.h>
# include <rdsn/internal/task_worker.h>
# include <rdsn/internal/admission_controller.h>
# include <rdsn/internal/network.h>
# include <rdsn/internal/aio_provider.h>
# include <rdsn/internal/env_provider.h>
# include <rdsn/internal/zlock_provider.h>
# include <rdsn/internal/zlocks.h>
# include <rdsn/internal/logging_provider.h>
# include <rdsn/internal/perf_counters.h>
# include <rdsn/internal/logging.h>

// other utilities
# include <rdsn/internal/service_app.h>

namespace rdsn { namespace tools {

class tool_base
{
public:
    tool_base(const char* name, configuration_ptr config);
    
protected:
    configuration_ptr _configuration;
    std::string      _name;
};

class toollet : public tool_base
{
public:
    template <typename T> static toollet* create(const char* name, configuration_ptr config)
    {
        return new T(name, config);
    }

public:
    toollet(const char* name, configuration_ptr config);

    virtual void install(service_spec& spec) = 0;
};

class tool_app : public tool_base
{
public:
    template <typename T> static tool_app* create(const char* name, configuration_ptr c)
    {
        return new T(name, c);
    }

public:
    tool_app(const char* name, configuration_ptr c);
    
    virtual void install(service_spec& spec) = 0;

    // this routine will be invoked in the main thread as the tool driver (if necessary for the tool, e.g., model checking)
    virtual void run() 
    { 
        start_all_service_apps(); 
    }

public:
    virtual void start_all_service_apps();
    virtual void stop_all_service_apps();
    
    static const service_spec& get_service_spec();
    static configuration_ptr config();
};

typedef task_queue*     (*task_queue_factory)(task_worker_pool*, int, task_queue*);
typedef task_worker*    (*task_worker_factory)(task_worker_pool*, task_queue*, int, task_worker*);
typedef admission_controller* (*admission_controller_factory)(task_queue*, const char*);
typedef lock_provider*   (*lock_factory)(rdsn::service::zlock *, lock_provider*);
typedef rwlock_provider* (*read_write_lock_factory)(rdsn::service::zrwlock *, rwlock_provider*);
typedef semaphore_provider* (*semaphore_factory)(rdsn::service::zsemaphore *, int, semaphore_provider*);
typedef network*       (*network_factory)(rpc_engine*, network*);
typedef aio_provider*   (*aio_factory)(disk_engine*, aio_provider*);
typedef env_provider*   (*env_factory)(env_provider*);

typedef perf_counter*    (*perf_counter_factory)(const char *, const char *, perf_counter_type);
typedef logging_provider* (*logging_factory)(const char*);
typedef toollet*       (*toollet_factory)(const char*, configuration_ptr);
typedef tool_app*       (*tool_app_factory)(const char*, configuration_ptr);

namespace internal_use_only
{
    bool register_component_provider(const char* name, task_queue_factory f, int type);
    bool register_component_provider(const char* name, task_worker_factory f, int type);
    bool register_component_provider(const char* name, admission_controller_factory f, int type);
    bool register_component_provider(const char* name, lock_factory f, int type);
    bool register_component_provider(const char* name, read_write_lock_factory f, int type);
    bool register_component_provider(const char* name, semaphore_factory f, int type);
    bool register_component_provider(const char* name, network_factory f, int type);
    bool register_component_provider(const char* name, aio_factory f, int type);
    bool register_component_provider(const char* name, env_factory f, int type);
    bool register_component_provider(const char* name, perf_counter_factory f, int type);
    bool register_component_provider(const char* name, logging_factory f, int type);
    
    bool register_toollet(const char* name, toollet_factory f, int type);
    bool register_tool(const char* name, tool_app_factory f, int type);
    toollet* get_toollet(const char* name, int type, configuration_ptr config);
}

extern join_point<void, const char*> syste_init;
extern join_point<long, syste_exit_type, int, void*> syste_exit; // return (see SetUnhandledExceptionFilter), type, error code, context

template <typename T> bool register_component_provider(const char* name) { return internal_use_only::register_component_provider(name, T::template create<T>, PROVIDER_TYPE_MAIN); }
template <typename T> bool register_component_aspect(const char* name) { return internal_use_only::register_component_provider(name, T::template create<T>, PROVIDER_TYPE_ASPECT); }
template <typename T> bool register_toollet(const char* name) { return internal_use_only::register_toollet(name, toollet::template create<T>, 0); }
template <typename T> bool register_tool(const char* name) { return internal_use_only::register_tool(name, tool_app::template create<T>, 0); }
template <typename T> T* get_toollet(const char* name) { return (T*)internal_use_only::get_toollet(name, 0, tool_app::config()); }

// --------- inline implementation -----------------------------


}} // end namespace rdsn::tool_api

