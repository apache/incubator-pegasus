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

# include "service_engine.h"
# include "task_engine.h"
# include "disk_engine.h"
# include "rpc_engine.h"
# include <dsn/internal/env_provider.h>
# include <dsn/internal/memory_provider.h>
# include <dsn/internal/nfs.h>
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/command.h>
# include <dsn/tool_api.h>
# include <dsn/tool/node_scoper.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "service_engine"

using namespace dsn::utils;

namespace dsn {

service_node::service_node(service_app_spec& app_spec)
{
    _computation = nullptr;
    _app_spec = app_spec;
    _app_context_ptr = nullptr;
}

bool service_node::rpc_register_handler(rpc_handler_ptr& handler, uint64_t vnid)
{
    for (auto& io : _ios)
    {
        if (io.rpc)
        {
            bool r = io.rpc->register_rpc_handler(handler, vnid);
            if (!r)
                return false;
        }
    }
    return true;
}

rpc_handler_ptr service_node::rpc_unregister_handler(dsn_task_code_t rpc_code, uint64_t vnid)
{
    rpc_handler_ptr ret = nullptr;
    for (auto& io : _ios)
    {
        if (io.rpc)
        {
            auto r = io.rpc->unregister_rpc_handler(rpc_code, vnid);
            if (ret != nullptr)
            {
                dassert(ret == r, "registered context must be the same");
            }
            else
            {
                ret = r;
            }
        }
    }
    return ret;
}

error_code service_node::init_io_engine(io_engine& io, ioe_mode mode)
{
    auto& spec = service_engine::fast_instance().spec();
    error_code err = ERR_OK;
    io_modifer ctx;
    ctx.queue = io.q;
    ctx.port_shift_value = 0; 
    ctx.mode = mode;
    
    // init timer service
    if (mode == spec.timer_io_mode)
    {
        io.tsvc = factory_store<timer_service>::create(
            service_engine::fast_instance().spec().timer_factory_name.c_str(),
            PROVIDER_TYPE_MAIN, this, nullptr);
        for (auto& s : service_engine::fast_instance().spec().timer_aspects)
        {
            io.tsvc = factory_store<timer_service>::create(
                s.c_str(),
                PROVIDER_TYPE_ASPECT,
                this, io.tsvc
                );
        }
    }
    else
        io.tsvc = nullptr;

    // init disk engine
    if (mode == spec.disk_io_mode)
    {
        io.disk = new disk_engine(this);
        aio_provider* aio = factory_store<aio_provider>::create(
            spec.aio_factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN, io.disk, nullptr);
        for (auto it = spec.aio_aspects.begin();
            it != spec.aio_aspects.end();
            it++)
        {
            aio = factory_store<aio_provider>::create(it->c_str(),
                PROVIDER_TYPE_ASPECT, io.disk, aio);
        }
        io.aio = aio;
    }
    else
        io.aio = nullptr;

    // init rpc engine
    if (mode == spec.rpc_io_mode)
    {
        if (ctx.mode == IOE_PER_QUEUE)
        {
            // update ports if there are more than one rpc engines for one node
            ctx.port_shift_value = spec.get_ports_delta(_app_spec.id, io.pool->spec().pool_code, io.q->index());
        }
        io.rpc = new rpc_engine(spec.config, this);
    }
    else
        io.rpc = nullptr;
    
    // init nfs
    io.nfs = nullptr;
    if (mode == spec.nfs_io_mode)
    {
        if (!spec.start_nfs)
        {
            ddebug("nfs not started coz [core] start_nfs = false");
        }
        else if (spec.nfs_factory_name == "")
        {
            dwarn("nfs not started coz no nfs_factory_name is specified,"
                " continue with no nfs");
        }
        else
        {
            io.nfs = factory_store<nfs_node>::create(spec.nfs_factory_name.c_str(),
                PROVIDER_TYPE_MAIN, this);
        }
    }

    return err;
}

error_code service_node::start_io_engine_in_main(const io_engine& io)
{
    auto& spec = service_engine::fast_instance().spec();
    error_code err = ERR_OK;
    io_modifer ctx;
    ctx.queue = io.q;
    ctx.port_shift_value = 0; 
    
    // start timer service    
    if (io.tsvc)
    {
        ctx.mode = spec.timer_io_mode;
        io.tsvc->start(ctx);
    }

    // start disk engine
    if (io.disk)
    {
        ctx.mode = spec.disk_io_mode;
        io.disk->start(io.aio, ctx);
    }

    // start rpc engine
    if (io.rpc)
    {
        ctx.mode = spec.rpc_io_mode;
        if (ctx.mode == IOE_PER_QUEUE)
        {
            // update ports if there are more than one rpc engines for one node
            ctx.port_shift_value = spec.get_ports_delta(_app_spec.id, io.pool->spec().pool_code, io.q->index());
        }
        err = io.rpc->start(_app_spec, ctx);
        if (err != ERR_OK) return err;
    }

    return err;
}


error_code service_node::start_io_engine_in_node_start_task(const io_engine& io)
{
    auto& spec = service_engine::fast_instance().spec();
    error_code err = ERR_OK;
    io_modifer ctx;
    ctx.queue = io.q;
    ctx.port_shift_value = 0;
    
    // start nfs delayed when the app is started
    if (io.nfs)
    {
        ctx.mode = spec.nfs_io_mode;
        if (ctx.mode == IOE_PER_QUEUE)
        {
            // update ports if there are more than one rpc engines for one node
            ctx.port_shift_value = spec.get_ports_delta(_app_spec.id, io.pool->spec().pool_code, io.q->index());
        }

        err = io.nfs->start(ctx);
        if (err != ERR_OK) return err;
    }

    return err;
}

dsn_error_t service_node::start_app(int argc, char** argv)
{    
    return _app_spec.role.start(_app_context_ptr, argc, argv);
}

error_code service_node::start()
{
    error_code err = ERR_OK;

    // init data dir
    if (!dsn::utils::filesystem::path_exists(spec().data_dir))
        dsn::utils::filesystem::create_directory(spec().data_dir);

    // init task engine    
    _computation = new task_engine(this);
    _computation->create(_app_spec.pools);    
    dassert (!_computation->is_started(), 
        "task engine must not be started at this point");

    // init per node io engines
    err = init_io_engine(_per_node_io, IOE_PER_NODE);
    if (err != ERR_OK) return err;
    _ios.push_back(_per_node_io);

    // init per queue io engines
    for (auto& pl : _computation->pools())
    {
        if (pl == nullptr)
            continue;

        for (auto& q : pl->queues())
        {
            io_engine io;
            io.q = q;
            io.pool = pl;

            err = init_io_engine(io, IOE_PER_QUEUE);
            if (err != ERR_OK) return err;
            _per_queue_ios[q] = io;

            _ios.push_back(io);
        }
    }

    // start io engines (only computation and timer), others are started in app start task
    for (auto& io : _ios)
    {
        start_io_engine_in_main(io);
    }

    // start task engine
    _computation->start();
    dassert(_computation->is_started(), 
        "task engine must be started at this point");

    // create app
    {
        ::dsn::tools::node_scoper scoper(this);
        _app_context_ptr = _app_spec.role.create(_app_spec.role.type_name.c_str());
    }

    return err;
}

void service_node::get_io(ioe_mode mode, task_queue* q, /*out*/ io_engine& io) const
{
    switch (mode)
    {
    case IOE_PER_NODE:
        io = _per_node_io;
        break;
    case IOE_PER_QUEUE:
        if (q)
        {
            auto it = _per_queue_ios.find(q);
            dassert(it != _per_queue_ios.end(), "io engine must be created for the queue");
            io = it->second;
        }
        else
        {
            // nothing to do
        }
        break;
    default:
        dassert(false, "invalid io mode");
    }
}
rpc_engine* service_node::rpc(task_queue* q) const
{
    auto& spec = service_engine::fast_instance().spec();
    io_engine io;
    get_io(spec.rpc_io_mode, q, io);
    return io.rpc;
}

disk_engine* service_node::disk(task_queue* q) const
{
    auto& spec = service_engine::fast_instance().spec();
    io_engine io;
    get_io(spec.disk_io_mode, q, io);
    return io.disk;
}

nfs_node* service_node::nfs(task_queue* q) const
{
    auto& spec = service_engine::fast_instance().spec();
    io_engine io;
    get_io(spec.nfs_io_mode, q, io);
    return io.nfs;
}

timer_service* service_node::tsvc(task_queue* q) const
{
    auto& spec = service_engine::fast_instance().spec();
    io_engine io;
    get_io(spec.timer_io_mode, q, io);
    return io.tsvc;
}

void service_node::get_runtime_info(
    const std::string& indent, 
    const std::vector<std::string>& args, 
    /*out*/ std::stringstream& ss
    )
{
    ss << indent << name() << ":" << std::endl;

    std::string indent2 = indent + "\t";
    _computation->get_runtime_info(indent2, args, ss);
}

void service_node::get_queue_info(
    /*out*/ std::stringstream& ss
    )
{
    ss << "{\"app_name\":\"" << name() << "\",\n\"thread_pool\":[\n";
    _computation->get_queue_info(ss);
    ss << "]}";
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
    ::dsn::register_command("system.queue", "system.queue - get queue internal information",
        "system.queue",
        &service_engine::get_queue_info
        );
}

void service_engine::init_before_toollets(const service_spec& spec)
{
    _spec = spec;

    // init common providers (first half)
    _logging = factory_store<logging_provider>::create(
        spec.logging_factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN, spec.dir_log.c_str()
        );
    _memory = factory_store<memory_provider>::create(
        spec.memory_factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN
        );
    perf_counters::instance().register_factory(
        factory_store<perf_counter>::get_factory<perf_counter::factory>(
        spec.perf_counter_factory_name.c_str(), ::dsn::PROVIDER_TYPE_MAIN
        )
        );
}

void service_engine::init_after_toollets()
{
    // init common providers (second half)
    _env = factory_store<env_provider>::create(_spec.env_factory_name.c_str(), 
        PROVIDER_TYPE_MAIN, nullptr);
    for (auto it = _spec.env_aspects.begin();
        it != _spec.env_aspects.end();
        it++)
    {
        _env = factory_store<env_provider>::create(it->c_str(), 
            PROVIDER_TYPE_ASPECT, _env);
    }
    tls_dsn.env = _env;
}

void service_engine::register_system_rpc_handler(
    dsn_task_code_t code, 
    const char* name, 
    dsn_rpc_request_handler_t cb, 
    void* param, 
    int port /*= -1*/
    ) // -1 for all node
{
    ::dsn::rpc_handler_ptr h(new ::dsn::rpc_handler_info(code));
    h->name = std::string(name);
    h->c_handler = cb;
    h->parameter = param;

    if (port == -1)
    {
        for (auto& n : _nodes_by_app_id)
        {
            for (auto& io : n.second->ios())
            {
                if (io.rpc)
                    io.rpc->register_rpc_handler(h, 0);
            }
        }
    }
    else
    {
        auto it = _nodes_by_app_port.find(port);
        if (it != _nodes_by_app_port.end())
        {
            for (auto& io : it->second->ios())
            {
                if (io.rpc)
                    io.rpc->register_rpc_handler(h, 0);
            }
        }
        else
        {
            dwarn("cannot find service node with port %d", port);
        }
    }
}

service_node* service_engine::start_node(service_app_spec& app_spec)
{
    auto it = _nodes_by_app_id.find(app_spec.id);
    if (it != _nodes_by_app_id.end())
    {
        return it->second;
    }
    else
    {
        for (auto p : app_spec.ports)
        {
            // union to existing node if any port is shared
            if (_nodes_by_app_port.find(p) != _nodes_by_app_port.end())
            {
                service_node* n = _nodes_by_app_port[p];

                dassert(false, "network port %d usage confliction for %s vs %s, "
                    "please reconfig",
                    p,
                    n->name(),
                    app_spec.name.c_str()
                    );
            }
        }
                
        auto node = new service_node(app_spec);
        error_code err = node->start();
        dassert (err == ERR_OK, "service node start failed, err = %s", err.to_string());
        
        _nodes_by_app_id[node->id()] = node;
        for (auto p1 : node->spec().ports)
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
        ss << "" << service_engine::fast_instance()._nodes_by_app_id.size() 
            << " nodes available:" << std::endl;
        for (auto& kv : service_engine::fast_instance()._nodes_by_app_id)
        {
            ss << "\t" << kv.second->id() << "." << kv.second->name() << std::endl;
        }
    }
    else
    {
        std::string indent = "";
        int id = atoi(args[0].c_str());
        auto it = service_engine::fast_instance()._nodes_by_app_id.find(id);
        if (it != service_engine::fast_instance()._nodes_by_app_id.end())
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

std::string service_engine::get_queue_info(const std::vector<std::string>& args)
{
    std::stringstream ss;
    ss << "[";
    for (auto &it : service_engine::fast_instance()._nodes_by_app_id)
    {
        if (it.first != service_engine::fast_instance()._nodes_by_app_id.begin()->first) ss << ",";
        it.second->get_queue_info(ss);
    }
    ss << "]";
    return ss.str();
}

void service_engine::configuration_changed()
{
    task_spec::init();
}

} // end namespace
