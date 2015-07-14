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
# pragma once

// providers
# include <dsn/internal/task_queue.h>
# include <dsn/internal/task_worker.h>
# include <dsn/internal/admission_controller.h>
# include <dsn/internal/network.h>
# include <dsn/internal/aio_provider.h>
# include <dsn/internal/env_provider.h>
# include <dsn/internal/nfs.h>
# include <dsn/internal/zlock_provider.h>
# include <dsn/internal/zlocks.h>
# include <dsn/internal/message_parser.h>
# include <dsn/internal/logging_provider.h>
# include <dsn/internal/memory_provider.h>
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/logging.h>
# include <dsn/internal/configuration.h>
# include <dsn/internal/memory.tools.h>

namespace dsn { namespace tools {
    
class tool_base
{
public:
    tool_base(const char* name);

    const std::string& name() const { return _name; }
    
protected:
    std::string _name;
};

class toollet : public tool_base
{
public:
    template <typename T> static toollet* create(const char* name)
    {
        return new T(name);
    }

public:
    toollet(const char* name);

    virtual void install(service_spec& spec) = 0;
};

class tool_app : public tool_base
{
public:
    template <typename T> static tool_app* create(const char* name)
    {
        return new T(name);
    }

public:
    tool_app(const char* name);
   
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
};

typedef task_queue*      (*task_queue_factory)(task_worker_pool*, int, task_queue*);
typedef task_worker*     (*task_worker_factory)(task_worker_pool*, task_queue*, int, task_worker*);
typedef admission_controller* (*admission_controller_factory)(task_queue*, const char*);
typedef lock_provider*   (*lock_factory)(dsn::service::zlock *, lock_provider*);
typedef rwlock_nr_provider* (*read_write_lock_factory)(dsn::service::zrwlock_nr *, rwlock_nr_provider*);
typedef semaphore_provider* (*semaphore_factory)(dsn::service::zsemaphore *, int, semaphore_provider*);
typedef network*         (*network_factory)(rpc_engine*, network*);
typedef aio_provider*    (*aio_factory)(disk_engine*, aio_provider*);
typedef env_provider*    (*env_factory)(env_provider*);
typedef nfs_node*        (*nfs_factory)(service_node*);
typedef message_parser*  (*message_parser_factory)(int);

typedef perf_counter*    (*perf_counter_factory)(const char *, const char *, perf_counter_type);
typedef logging_provider* (*logging_factory)();
typedef memory_provider* (*memory_factory)();
typedef toollet*         (*toollet_factory)(const char*);
typedef tool_app*        (*tool_app_factory)(const char*);

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
    bool register_component_provider(const char* name, memory_factory f, int type);
    bool register_component_provider(const char* name, nfs_factory f, int type);
    bool register_component_provider(const char* name, message_parser_factory f, int type);
    
    bool register_toollet(const char* name, toollet_factory f, int type);
    bool register_tool(const char* name, tool_app_factory f, int type);
    toollet* get_toollet(const char* name, int type);
}

extern join_point<void, configuration_ptr> sys_init_before_app_created;
extern join_point<void, configuration_ptr> sys_init_after_app_created;
extern join_point<void, sys_exit_type>     sys_exit;

template <typename T> bool register_component_provider(const char* name) { return internal_use_only::register_component_provider(name, T::template create<T>, PROVIDER_TYPE_MAIN); }
template <typename T> bool register_component_aspect(const char* name) { return internal_use_only::register_component_provider(name, T::template create<T>, PROVIDER_TYPE_ASPECT); }
template <typename T> bool register_message_header_parser(network_header_format fmt) { return internal_use_only::register_component_provider(fmt.to_string(), T::template create<T>, PROVIDER_TYPE_MAIN); }

template <typename T> bool register_toollet(const char* name) { return internal_use_only::register_toollet(name, toollet::template create<T>, 0); }
template <typename T> bool register_tool(const char* name) { return internal_use_only::register_tool(name, tool_app::template create<T>, 0); }
template <typename T> T* get_toollet(const char* name) { return (T*)internal_use_only::get_toollet(name, 0); }
tool_app* get_current_tool();
configuration_ptr config();

// --------- inline implementation -----------------------------


}} // end namespace dsn::tools

