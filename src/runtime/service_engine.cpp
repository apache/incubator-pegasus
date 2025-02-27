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

#include "service_engine.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <functional>
#include <list>
#include <unordered_map>
#include <utility>

#include "common/gpid.h"
#include "fmt/core.h"
#include "nlohmann/json.hpp"
#include "rpc/rpc_engine.h"
#include "rpc/rpc_message.h"
#include "runtime/node_scoper.h"
#include "task/task.h"
#include "task/task_engine.h"
#include "task/task_spec.h"
#include "utils/command_manager.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

namespace dsn {

service_node::service_node(service_app_spec &app_spec) : _app_spec(app_spec) {}

bool service_node::rpc_register_handler(task_code code,
                                        const char *extra_name,
                                        const rpc_request_handler &h)
{
    return _rpc->register_rpc_handler(code, extra_name, h);
}

bool service_node::rpc_unregister_handler(dsn::task_code rpc_code)
{
    return _rpc->unregister_rpc_handler(rpc_code);
}

error_code service_node::init_rpc_engine()
{
    // init rpc engine
    _rpc = std::make_unique<rpc_engine>(this);

    // start rpc engine
    return _rpc->start(_app_spec);
}

dsn::error_code service_node::start_app()
{
    CHECK(_entity, "entity hasn't initialized");
    _entity->set_host_port(rpc()->primary_host_port());

    std::vector<std::string> args;
    utils::split_args(spec().arguments.c_str(), args);
    args.insert(args.begin(), spec().full_name);
    dsn::error_code res = _entity->start(args);
    if (res == dsn::ERR_OK) {
        _entity->set_started(true);
    }
    return res;
}

dsn::error_code service_node::stop_app(bool cleanup)
{
    CHECK(_entity, "entity hasn't initialized");
    dsn::error_code res = _entity->stop(cleanup);
    if (res == dsn::ERR_OK) {
        _entity->set_started(false);
    }
    return res;
}

void service_node::init_service_app()
{
    _info.entity_id = _app_spec.id;
    _info.index = _app_spec.index;
    _info.role_name = _app_spec.role_name;
    _info.type = _app_spec.type;
    _info.full_name = _app_spec.full_name;
    _info.data_dir = _app_spec.data_dir;

    _entity.reset(service_app::new_service_app(_app_spec.type, &_info));
}

error_code service_node::start()
{
    error_code err = ERR_OK;

    // init data dir
    if (!dsn::utils::filesystem::path_exists(spec().data_dir))
        dsn::utils::filesystem::create_directory(spec().data_dir);

    // init task engine
    _computation = std::make_unique<task_engine>(this);
    _computation->create(_app_spec.pools);
    CHECK(!_computation->is_started(), "task engine must not be started at this point");

    // init rpc
    err = init_rpc_engine();
    if (err != ERR_OK)
        return err;

    // start task engine
    _computation->start();
    CHECK(_computation->is_started(), "task engine must be started at this point");

    // create service_app
    {
        ::dsn::tools::node_scoper scoper(this);
        init_service_app();
    }

    // start rpc serving
    _rpc->start_serving();

    return err;
}

std::string service_node::get_runtime_info(const std::vector<std::string> &args) const
{
    nlohmann::json info;
    info[full_name()] = _computation->get_runtime_info(args);
    return info.dump(2);
}

nlohmann::json service_node::get_queue_info() const
{
    nlohmann::json info;
    info["app_name"] = full_name();
    info["thread_pool"] = _computation->get_queue_info();
    return info;
}

rpc_request_task *service_node::generate_intercepted_request_task(message_ex *req)
{
    bool is_write = task_spec::get(req->local_rpc_code)->rpc_request_is_write_operation;
    rpc_request_task *t = new rpc_request_task(req,
                                               std::bind(&service_app::on_intercepted_request,
                                                         _entity.get(),
                                                         req->header->gpid,
                                                         is_write,
                                                         std::placeholders::_1),
                                               this);
    t->spec().on_task_create.execute(nullptr, t);
    return t;
}

service_node::~service_node()
{
    _rpc->stop_serving();
    stop_app(false);
    _computation->stop();
}

//////////////////////////////////////////////////////////////////////////////////////////

service_engine::service_engine()
{
    _env = nullptr;

    _cmds.emplace_back(dsn::command_manager::instance().register_single_command(
        "engine",
        "Get engine internal information, including threadpools and threads and queues in each "
        "threadpool",
        "[app-id]",
        &service_engine::get_runtime_info));

    _cmds.emplace_back(dsn::command_manager::instance().register_single_command(
        "system.queue",
        "Get queue internal information, including the threadpool each queue belongs to, and the "
        "queue name and size",
        "",
        &service_engine::get_queue_info));
}

service_engine::~service_engine() { _nodes_by_app_id.clear(); }

void service_engine::init_before_toollets(const service_spec &spec) { _spec = spec; }

void service_engine::init_after_toollets()
{
    // init common providers (second half)
    _env = utils::factory_store<env_provider>::create(
        _spec.env_factory_name.c_str(), PROVIDER_TYPE_MAIN, nullptr);
    tls_dsn.env = _env;
}

void service_engine::start_node(service_app_spec &app_spec)
{
    std::unordered_map<int, std::string> app_name_by_port;
    auto it = _nodes_by_app_id.find(app_spec.id);
    if (it == _nodes_by_app_id.end()) {
        for (auto p : app_spec.ports) {
            // union to existing node if any port is shared
            auto it = app_name_by_port.find(p);
            if (it != app_name_by_port.end()) {
                CHECK(false,
                      "network port {} usage confliction for {} vs {}, "
                      "please reconfig",
                      p,
                      it->second,
                      app_spec.full_name);
            }
            app_name_by_port.emplace(p, app_spec.full_name);
        }

        auto node = std::make_shared<service_node>(app_spec);
        error_code err = node->start();
        CHECK_EQ_MSG(err, ERR_OK, "service node start failed");

        _nodes_by_app_id[node->id()] = node;
    }
}

std::string service_engine::get_runtime_info(const std::vector<std::string> &args)
{
    // Overview.
    if (args.empty()) {
        nlohmann::json overview;
        nlohmann::json nodes;
        for (const auto &nodes_by_app_id : service_engine::instance()._nodes_by_app_id) {
            nodes.emplace_back(fmt::format(
                "{}.{}", nodes_by_app_id.second->id(), nodes_by_app_id.second->full_name()));
        }
        overview["available_nodes"] = nodes;
        return overview.dump(2);
    }

    // Invalid argument.
    int id;
    if (!dsn::buf2int32(args[0], id)) {
        nlohmann::json err_msg;
        err_msg["error"] = "invalid argument, only one integer argument is acceptable";
        return err_msg.dump(2);
    }

    // The query id is not found.
    const auto &it = service_engine::instance()._nodes_by_app_id.find(id);
    if (it == service_engine::instance()._nodes_by_app_id.end()) {
        nlohmann::json err_msg;
        err_msg["error"] = fmt::format("cannot find node with given app id({})", id);
        return err_msg.dump(2);
    }

    // Query a special id.
    auto tmp_args = args;
    tmp_args.erase(tmp_args.begin());
    return it->second->get_runtime_info(tmp_args);
}

std::string service_engine::get_queue_info(const std::vector<std::string> &args)
{
    nlohmann::json info;
    for (const auto &nodes_by_app_id : service_engine::instance()._nodes_by_app_id) {
        info.emplace_back(nodes_by_app_id.second->get_queue_info());
    }
    return info.dump(2);
}

bool service_engine::is_simulator() const { return _simulator; }

void service_engine::set_simulator() { _simulator = true; }

} // namespace dsn
