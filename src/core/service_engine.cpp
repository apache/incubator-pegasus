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
# include <dsn/internal/nfs.h>
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/logging.h>
# include <dsn/tool_api.h>

#define __TITLE__ "service_engine"

using namespace dsn::utils;

namespace dsn {

    service_node::service_node(int app_id, const std::string& app_name)
{
    _computation = nullptr;
    _rpc = nullptr;
    _disk = nullptr;
    _nfs = nullptr;
    _app_id = app_id;
    _app_name = app_name;
}

error_code service_node::start(const std::vector<int>& ports)
{
    auto& spec = service_engine::instance().spec();

    // init task engine    
    _computation = new task_engine(this);
    _computation->start(spec.threadpool_specs);    
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
    error_code err = _rpc->start(_app_id, ports);
    if (err != ERR_SUCCESS) return err;

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

//////////////////////////////////////////////////////////////////////////////////////////

static std::map<int, service_node*>* s_nodes;

service_engine::service_engine(void)
{
    _env = nullptr;
    _logging = nullptr;
    s_nodes = &_engines_by_app_id;
}

void service_engine::init_before_toollets(const service_spec& spec)
{
    _spec = spec;

    // init common providers (first half)
    _logging = factory_store<logging_provider>::create(spec.logging_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);
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

service_node* service_engine::start_node(int app_id, const std::string& app_name, const std::vector<int>& ports)
{
    auto it = _engines_by_app_id.find(app_id);
    if (it != _engines_by_app_id.end())
    {
        return it->second;
    }
    else
    {
        for (auto p : ports)
        {
            // union to existing node if any port is shared
            if (_engines_by_port.find(p) != _engines_by_port.end())
            {
                service_node* n = _engines_by_port[p];

                for (auto p1 : ports)
                {
                    if (n->rpc()->start_server_port(p1))
                        _engines_by_port[p1] = n;
                }
                return n;
            }
        }
        
        auto node = new service_node(app_id, app_name);
        error_code err = node->start(ports);
        dassert (err == 0, "service node start failed, err = %s", err.to_string());
        
        _engines_by_app_id[app_id] = node;
        for (auto p1 : ports)
        {
            _engines_by_port[p1] = node;
        }

        return node;
    }
}

void service_engine::configuration_changed(configuration_ptr configuration)
{
    task_spec::init(configuration);
}

} // end namespace
