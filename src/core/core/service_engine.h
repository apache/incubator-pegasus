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

# include <dsn/ports.h>
# include <dsn/internal/singleton.h>
# include <dsn/internal/global_config.h>
# include <dsn/cpp/auto_codes.h>
# include <sstream>
# include <dsn/internal/synchronize.h>

namespace dsn { 

class task_engine;
class rpc_engine;
class disk_engine;
class env_provider;
class logging_provider;
class nfs_node;
class memory_provider;
class task_queue;
class task_worker_pool;

class service_node
{
public:
    struct io_engine
    {
        rpc_engine*      rpc;
        disk_engine*     disk;
        nfs_node*        nfs;

        io_engine()
        {
            rpc = nullptr;
            disk = nullptr;
            nfs = nullptr;
        }
    };

public:    
    service_node(service_app_spec& app_spec, void* app_context);
       
    rpc_engine*  rpc(task_worker_pool* pool, task_queue* q) const;
    disk_engine* disk(task_worker_pool* pool, task_queue* q) const;
    nfs_node* nfs(task_worker_pool* pool, task_queue* q) const;

    task_engine* computation() const { return _computation; }
    const std::list<io_engine>& ios() const { return _ios; }
    void get_runtime_info(const std::string& indent, const std::vector<std::string>& args, __out_param std::stringstream& ss);

    ::dsn::error_code start();

    int id() const { return _app_spec.id; }
    const char* name() const { return _app_spec.name.c_str(); }
    const service_app_spec& spec() const { return _app_spec;  }
    void* get_app_context_ptr() const { return _app_context_ptr; }

    void* get_per_node_state(const char* name);
    bool  put_per_node_state(const char* name, void* obj);
    void* remove_per_node_state(const char* name);

    bool  rpc_register_handler(rpc_handler_ptr& handler);
    rpc_handler_ptr rpc_unregister_handler(dsn_task_code_t rpc_code);

private:
    void*            _app_context_ptr; // app start returns this value and used by app stop
    service_app_spec _app_spec;
    task_engine*     _computation;

    io_loop_mode                                _io_mode;
    io_engine                                   _per_node_io;
    std::unordered_map<task_worker_pool*, io_engine> _per_pool_ios;
    std::unordered_map<task_queue*, io_engine>  _per_queue_ios;
    std::list<io_engine>                        _ios; // all ios
    
    utils::ex_lock_nr_spin                      _lock;
    std::unordered_map<std::string, void*>      _per_node_states;
    
private:
    error_code init_io_engine(io_engine& io, int ports_add);
    void get_io(task_worker_pool* pool, task_queue* q, __out_param io_engine& io) const;
};

typedef std::map<int, service_node*> service_nodes_by_app_id;
class service_engine : public utils::singleton<service_engine>
{
public:
    service_engine();

    //ServiceMode Mode() const { return _spec.Mode; }
    const service_spec& spec() const { return _spec; }
    env_provider* env() const { return _env; }
    logging_provider* logging() const { return _logging; }
    memory_provider* memory() const { return _memory; }
    static std::string get_runtime_info(const std::vector<std::string>& args);
        
    void init_before_toollets(const service_spec& spec);
    void init_after_toollets();
    void configuration_changed();

    service_node* start_node(service_app_spec& app_spec);
    void register_system_rpc_handler(dsn_task_code_t code, const char* name, dsn_rpc_request_handler_t cb, void* param, int port = -1); // -1 for all nodes
    const service_nodes_by_app_id& get_all_nodes() const { return _nodes_by_app_id; }

private:
    service_spec                    _spec;
    env_provider*                   _env;
    logging_provider*               _logging;
    memory_provider*                _memory;

    // <port, servicenode>    
    typedef std::map<int, service_node*> node_engines_by_port; // multiple ports may share the same node
    service_nodes_by_app_id         _nodes_by_app_id;
    node_engines_by_port            _nodes_by_app_port;
};

// ------------ inline impl ---------------------

} // end namespace
