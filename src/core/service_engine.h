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

# include <dsn/internal/dsn_types.h>
# include <dsn/internal/singleton.h>
# include <dsn/internal/end_point.h>
# include <dsn/internal/global_config.h>
# include <dsn/internal/error_code.h>
# include <sstream>

namespace dsn { 

class task_engine;
class rpc_engine;
class disk_engine;
class env_provider;
class logging_provider;
class nfs_node;
class memory_provider;

namespace service {
    class service_app;
}

class service_node
{
public:    
    service_node(service::service_app* app);
    
    task_engine* computation() const { return _computation; }
    rpc_engine*  rpc() const { return _rpc; }
    disk_engine* disk() const { return _disk; }
    nfs_node* nfs() const { return _nfs; }
    void get_runtime_info(const std::string& indent, const std::vector<std::string>& args, __out_param std::stringstream& ss);

    error_code start();

    int id() const { return _app_id; }
    const char* name() const { return _app_name.c_str(); }
    const service_app_spec& spec() const;
    
private:
    int          _app_id;
    std::string  _app_name;
    service::service_app  *_app;
    task_engine* _computation;
    rpc_engine*  _rpc;
    disk_engine* _disk;
    nfs_node*    _nfs;
};

class rpc_server_handler;
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
    void configuration_changed(configuration_ptr configuration);

    service_node* start_node(service::service_app* app);
    void register_system_rpc_handler(task_code code, const char* name, rpc_server_handler* handler, int port = -1); // -1 for all nodes
    
private:
    service_spec                    _spec;
    env_provider*                   _env;
    logging_provider*               _logging;
    memory_provider*                _memory;

    // <port, servicenode>
    typedef std::map<int, service_node*> node_engines_by_app_id;
    typedef std::map<int, service_node*> node_engines_by_port; // multiple ports may share the same node
    node_engines_by_app_id          _nodes_by_app_id;
    node_engines_by_port            _nodes_by_app_port;
};

// ------------ inline impl ---------------------

} // end namespace
