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

# pragma once

# include <dsn/internal/ports.h>
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
class timer_service;
class aio_provider;
class rpc_server_dispatcher;
class service_node;

class layer2_handler_core
{
public:
    layer2_handler_core(service_node* node);

    error_code create_layer1_app(dsn_gpid gpid, /*our*/ void** app_context);

    error_code start_layer1_app(void* app_context, int argc, char** argv);

    void destroy_layer1_app(void* app_context, bool cleanup);

    bool  rpc_register_handler(void* app_context, rpc_handler_info* handler);

    rpc_handler_info* rpc_unregister_handler(void* app_context, dsn_task_code_t rpc_code);

    void commit_layer1(void* app_context, dsn_message_t msg);

private:
    struct layer1_app_info
    {
        void*                 app_context;
        dsn_gpid              gpid;
        std::unique_ptr<rpc_server_dispatcher> server_dispatcher;
    };

    service_node  *_owner_node;
    utils::rw_lock_nr _apps_lock;
    std::unordered_map<uint64_t, std::unique_ptr<layer1_app_info> > _layer1_apps;
};

//
//
//
class service_node
{
public:
    struct io_engine
    {        
        rpc_engine*      rpc;
        disk_engine*     disk;
        nfs_node*        nfs;
        timer_service*   tsvc;

        task_queue*      q;
        task_worker_pool *pool;
        aio_provider     *aio;

        io_engine()
        {
            memset((void*)this, 0, sizeof(io_engine));
        }
    };

public:    
    service_node(service_app_spec& app_spec);
       
    rpc_engine*  rpc(task_queue* q) const;
    disk_engine* disk(task_queue* q) const;
    nfs_node* nfs(task_queue* q) const;
    timer_service* tsvc(task_queue* q) const;

    rpc_engine*  node_rpc() const { return _per_node_io.rpc; }
    disk_engine* node_disk() const { return _per_node_io.disk; }
    nfs_node* node_nfs() const { return _per_node_io.nfs; }
    timer_service* node_tsvc() const { return _per_node_io.tsvc; }

    task_engine* computation() const { return _computation; }
    const std::list<io_engine>& ios() const { return _ios; }
    void get_runtime_info(const std::string& indent, const std::vector<std::string>& args, /*out*/ std::stringstream& ss);
    void get_queue_info(/*out*/ std::stringstream& ss);

    error_code start_io_engine_in_node_start_task(const io_engine& io);

    ::dsn::error_code start();
    dsn_error_t start_app(int argc, char** argv);

    int id() const { return _app_spec.id; }
    const char* name() const { return _app_spec.name.c_str(); }
    const service_app_spec& spec() const { return _app_spec;  }
    void* get_app_context_ptr() const { return _app_context_ptr; }

    bool  rpc_register_handler(rpc_handler_info* handler, void* layer1_app_context = nullptr);
    rpc_handler_info* rpc_unregister_handler(dsn_task_code_t rpc_code, void* layer1_app_context = nullptr);

    layer2_handler_core& get_l2_handler() { return _layer2_handler; }
    void handle_l2_rpc_request(dsn_gpid gpid, bool is_write, dsn_message_t req, int delay);

private:
    void*            _app_context_ptr; // app start returns this value and used by app stop
    service_app_spec _app_spec;
    task_engine*     _computation;

    io_engine                                   _per_node_io;
    std::unordered_map<task_queue*, io_engine>  _per_queue_ios;
    std::list<io_engine>                        _ios; // all ios

    // when this app is hosted by a layer2 handler app
    layer2_handler_core                         _layer2_handler;
    dsn_app                                     *_layer2_role;

private:
    error_code init_io_engine(io_engine& io, ioe_mode mode);
    error_code start_io_engine_in_main(const io_engine& io);
    void get_io(ioe_mode mode, task_queue* q, /*out*/ io_engine& io) const;
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
    static std::string get_queue_info(const std::vector<std::string>& args);

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
