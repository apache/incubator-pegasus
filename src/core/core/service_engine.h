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

#pragma once

#include <sstream>

#include <dsn/utility/ports.h>
#include <dsn/utility/singleton.h>
#include <dsn/tool-api/global_config.h>
#include <dsn/tool-api/task.h>
#include <dsn/cpp/auto_codes.h>
#include <dsn/utility/synchronize.h>

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

//
//
//
class service_node
{
public:
    struct io_engine
    {
        rpc_engine *rpc;
        disk_engine *disk;
        nfs_node *nfs;
        timer_service *tsvc;

        task_queue *q;
        task_worker_pool *pool;
        aio_provider *aio;

        io_engine() { memset((void *)this, 0, sizeof(io_engine)); }
    };

public:
    explicit service_node(service_app_spec &app_spec);

    rpc_engine *rpc(task_queue *q) const;
    disk_engine *disk(task_queue *q) const;
    nfs_node *nfs(task_queue *q) const;
    timer_service *tsvc(task_queue *q) const;

    rpc_engine *node_rpc() const { return _per_node_io.rpc; }
    disk_engine *node_disk() const { return _per_node_io.disk; }
    nfs_node *node_nfs() const { return _per_node_io.nfs; }
    timer_service *node_tsvc() const { return _per_node_io.tsvc; }

    task_engine *computation() const { return _computation; }
    const std::list<io_engine> &ios() const { return _ios; }
    void get_runtime_info(const std::string &indent,
                          const std::vector<std::string> &args,
                          /*out*/ std::stringstream &ss);
    void get_queue_info(/*out*/ std::stringstream &ss);

    error_code start_io_engine_in_node_start_task(const io_engine &io);

    ::dsn::error_code start();
    dsn_error_t start_app();

    int id() const { return _app_spec.id; }
    const char *name() const { return _app_spec.name.c_str(); }
    const service_app_spec &spec() const { return _app_spec; }
    void *get_app_context_ptr() const { return _app_info.app.app_context_ptr; }

    bool rpc_register_handler(rpc_handler_info *handler, dsn_gpid gpid);
    rpc_handler_info *rpc_unregister_handler(dsn_task_code_t rpc_code, dsn_gpid gpid);

    dsn_app_info *get_app_info() { return &_app_info; }
    void handle_intercepted_request(dsn_gpid gpid, bool is_write, dsn_message_t req);
    rpc_request_task *generate_intercepted_request_task(message_ex *req);

    static dsn_error_t start_app(void *app_context,
                                 const std::string &args,
                                 dsn_app_start start,
                                 const std::string &app_name);

private:
    dsn_app_info _app_info;

    service_app_spec _app_spec;
    task_engine *_computation;

    io_engine _per_node_io;
    std::unordered_map<task_queue *, io_engine> _per_queue_ios;
    std::list<io_engine> _ios; // all ios

    rpc_handler_info _intercepted_read;
    rpc_handler_info _intercepted_write;

private:
    error_code init_io_engine(io_engine &io, ioe_mode mode);
    error_code start_io_engine_in_main(const io_engine &io);
    void get_io(ioe_mode mode, task_queue *q, /*out*/ io_engine &io) const;
};

typedef std::map<int, service_node *> service_nodes_by_app_id;
class service_engine : public utils::singleton<service_engine>
{
public:
    service_engine();

    // ServiceMode Mode() const { return _spec.Mode; }
    const service_spec &spec() const { return _spec; }
    env_provider *env() const { return _env; }
    logging_provider *logging() const { return _logging; }
    memory_provider *memory() const { return _memory; }
    static std::string get_runtime_info(const std::vector<std::string> &args);
    static std::string get_queue_info(const std::vector<std::string> &args);

    void init_before_toollets(const service_spec &spec);
    void init_after_toollets();
    void configuration_changed();

    service_node *start_node(service_app_spec &app_spec);
    void register_system_rpc_handler(dsn_task_code_t code,
                                     const char *name,
                                     dsn_rpc_request_handler_t cb,
                                     void *param,
                                     int port = -1); // -1 for all nodes
    const service_nodes_by_app_id &get_all_nodes() const { return _nodes_by_app_id; }

private:
    service_spec _spec;
    env_provider *_env;
    logging_provider *_logging;
    memory_provider *_memory;

    // <port, servicenode>
    typedef std::map<int, service_node *>
        node_engines_by_port; // multiple ports may share the same node
    service_nodes_by_app_id _nodes_by_app_id;
    node_engines_by_port _nodes_by_app_port;
};

// ------------ inline impl ---------------------

} // end namespace
