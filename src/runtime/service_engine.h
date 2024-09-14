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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "nlohmann/json_fwd.hpp"
#include "runtime/api_task.h"
#include "runtime/global_config.h"
#include "runtime/service_app.h"
#include "task/task_code.h"
#include "utils/error_code.h"
#include "utils/singleton.h"

namespace dsn {

class env_provider;
class message_ex;
class rpc_engine;
class rpc_request_task;
class task_engine;

//
//
//
class service_node
{
public:
    explicit service_node(service_app_spec &app_spec);

    ~service_node();

    rpc_engine *rpc() const { return _rpc.get(); }
    task_engine *computation() const { return _computation.get(); }

    std::string get_runtime_info(const std::vector<std::string> &args) const;
    nlohmann::json get_queue_info() const;

    dsn::error_code start();
    dsn::error_code start_app();
    dsn::error_code stop_app(bool cleanup);

    int id() const { return _app_spec.id; }
    const char *full_name() const { return _app_spec.full_name.c_str(); }
    const service_app_spec &spec() const { return _app_spec; }
    const service_app_info &get_service_app_info() const { return _info; }
    const service_app *get_service_app() const { return _entity.get(); }
    bool rpc_register_handler(task_code code, const char *extra_name, const rpc_request_handler &h);
    bool rpc_unregister_handler(task_code rpc_code);

    rpc_request_task *generate_intercepted_request_task(message_ex *req);

private:
    service_app_info _info;
    std::unique_ptr<service_app> _entity;

    service_app_spec _app_spec;

    std::unique_ptr<task_engine> _computation;
    std::unique_ptr<rpc_engine> _rpc;

private:
    // the service entity is initialized after the engine
    // is initialized, so this should be call in start()
    void init_service_app();

    error_code init_rpc_engine();
};

class command_deregister;

typedef std::map<int, std::shared_ptr<service_node>> service_nodes_by_app_id;
class service_engine : public utils::singleton<service_engine>
{
public:
    // ServiceMode Mode() const { return _spec.Mode; }
    const service_spec &spec() const { return _spec; }
    env_provider *env() const { return _env; }
    static std::string get_runtime_info(const std::vector<std::string> &args);
    static std::string get_queue_info(const std::vector<std::string> &args);

    void init_before_toollets(const service_spec &spec);
    void init_after_toollets();

    void start_node(service_app_spec &app_spec);
    const service_nodes_by_app_id &get_all_nodes() const { return _nodes_by_app_id; }
    bool is_simulator() const;
    void set_simulator();

private:
    service_engine();
    ~service_engine();

    service_spec _spec;
    env_provider *_env;

    std::vector<std::unique_ptr<command_deregister>> _cmds;

    bool _simulator;

    // map app_id to service_node
    service_nodes_by_app_id _nodes_by_app_id;

    friend class utils::singleton<service_engine>;
};

// ------------ inline impl ---------------------

} // namespace dsn
