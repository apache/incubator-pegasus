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
# include "service_engine.h"
# include "task_engine.h"
# include "disk_engine.h"
# include "rpc_engine.h"
# include <dsn/internal/env_provider.h>
# include <dsn/internal/memory_provider.h>
# include <dsn/internal/nfs.h>
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/logging.h>
# include <dsn/tool_api.h>
# include <dsn/internal/service_app.h>
# include <dsn/internal/command.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "service_engine"

using namespace dsn::utils;

namespace dsn {

service_node::service_node(service::service_app* app)
{
    _computation = nullptr;
    _rpc = nullptr;
    _disk = nullptr;
    _nfs = nullptr;
    _app_id = app->id();
    _app_name = app->name();
    _app = app;
}

error_code service_node::start()
{
    auto& spec = service_engine::instance().spec();

    // init task engine    
    _computation = new task_engine(this);
    _computation->start(_app->spec().pools);    
    dassert (_computation->is_started(), "task engine must be started at this point");

    // init disk engine
    _disk = new disk_engine(this);
    aio_provider* aio = factory_store<aio_provider>::create(spec.aio_factory_name.c_str(), PROVIDER_TYPE_MAIN, _disk, nullptr);
    for (auto it = spec.aio_aspects.begin();
        it != spec.aio_aspects.end();
        it++)
    {
        aio = factory_store<aio_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, _disk, aio);
    }
    _disk->start(aio);
    
    // init rpc engine
    _rpc = new rpc_engine(spec.config, this);    
    error_code err = _rpc->start(_app->spec());
    if (err != ERR_OK) return err;

    // init nfs
    if (spec.nfs_factory_name == "")
    {
        dwarn ("nfs not started coz no nfs_factory_name is specified, continue with no nfs");
    }
    else
    {
        _nfs = factory_store<nfs_node>::create(spec.nfs_factory_name.c_str(), PROVIDER_TYPE_MAIN, this);
    }

    return err;
}

const service_app_spec& service_node::spec() const
{
    return _app->spec();
}

void service_node::get_runtime_info(const std::string& indent, const std::vector<std::string>& args, __out_param std::stringstream& ss)
{
    ss << indent << name() << ":" << std::endl;

    std::string indent2 = indent + "\t";
    _computation->get_runtime_info(indent2, args, ss);
}

//////////////////////////////////////////////////////////////////////////////////////////

service_engine::service_engine(void)
{
    _env = nullptr;
    _logging = nullptr;
    _memory = nullptr;

    ::dsn::register_command("engine", "engine - get engine internal information",
        "engine [app-id]",
        &service_engine::get_runtime_info
        );
}

void service_engine::init_before_toollets(const service_spec& spec)
{
    _spec = spec;

    // init common providers (first half)
    _logging = factory_store<logging_provider>::create(spec.logging_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);
    _memory = factory_store<memory_provider>::create(spec.memory_factory_name.c_str(), PROVIDER_TYPE_MAIN);
    perf_counters::instance().register_factory(factory_store<perf_counter>::get_factory<perf_counter_factory>(spec.perf_counter_factory_name.c_str(), PROVIDER_TYPE_MAIN));
}

void service_engine::init_after_toollets()
{
    // init common providers (second half)
    _env = factory_store<env_provider>::create(_spec.env_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);
    for (auto it = _spec.env_aspects.begin();
        it != _spec.env_aspects.end();
        it++)
    {
        _env = factory_store<env_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, _env);
    }
}

void service_engine::register_system_rpc_handler(task_code code, const char* name, rpc_server_handler* handler, int port /*= -1*/) // -1 for all node
{
    rpc_handler_ptr h(new rpc_handler_info(code));
    h->name = std::string(name);
    h->handler = handler;

    if (port == -1)
    {
        for (auto& n : _nodes_by_app_id)
        {
            n.second->rpc()->register_rpc_handler(h);
        }
    }
    else
    {
        auto it = _nodes_by_app_port.find(port);
        if (it != _nodes_by_app_port.end())
        {
            it->second->rpc()->register_rpc_handler(h);
        }
        else
        {
            dwarn("cannot find service node with port %d", port);
        }
    }
}

service_node* service_engine::start_node(service::service_app* app)
{
    auto it = _nodes_by_app_id.find(app->id());
    if (it != _nodes_by_app_id.end())
    {
        return it->second;
    }
    else
    {
        for (auto p : app->spec().ports)
        {
            // union to existing node if any port is shared
            if (_nodes_by_app_port.find(p) != _nodes_by_app_port.end())
            {
                service_node* n = _nodes_by_app_port[p];

                dassert(false, "network port %d usage confliction for %s vs %s, please reconfig",
                    p,
                    n->name(),
                    app->name().c_str()
                    );
            }
        }
        
        auto node = new service_node(app);
        error_code err = node->start();
        dassert (err == 0, "service node start failed, err = %s", err.to_string());
        
        _nodes_by_app_id[app->id()] = node;
        for (auto p1 : app->spec().ports)
        {
            _nodes_by_app_port[p1] = node;
        }

        return node;
    }
}

std::string service_engine::get_runtime_info(const std::vector<std::string>& args)
{
    std::stringstream ss;
    if (args.size() == 0)
    {
        ss << "" << service_engine::instance()._nodes_by_app_id.size() << " nodes available:" << std::endl;
        for (auto& kv : service_engine::instance()._nodes_by_app_id)
        {
            ss << "\t" << kv.second->id() << "." << kv.second->name() << std::endl;
        }
    }
    else
    {
        std::string indent = "";
        int id = atoi(args[0].c_str());
        auto it = service_engine::instance()._nodes_by_app_id.find(id);
        if (it != service_engine::instance()._nodes_by_app_id.end())
        {
            auto args2 = args;
            args2.erase(args2.begin());
            it->second->get_runtime_info(indent, args2, ss);
        }
        else
        {
            ss << "cannot find node with given app id";
        }
    }
    return ss.str();
}

void service_engine::configuration_changed(configuration_ptr configuration)
{
    task_spec::init(configuration);
}

} // end namespace
