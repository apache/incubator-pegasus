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

#include "runtime/tool_api.h"

#include <functional>
#include <map>
#include <memory>
#include <type_traits>
#include <utility>

#include "rpc/message_parser_manager.h"
#include "runtime/global_config.h"
#include "runtime/service_engine.h"
#include "task/task.h"
#include "task/task_code.h"
#include "utils/error_code.h"
#include "utils/factory_store.h"
#include "utils/fmt_logging.h"
#include "utils/singleton_store.h"
#include "utils/sys_exit_hook.h"
#include "utils/threadpool_code.h"

namespace dsn {

DEFINE_TASK_CODE(LPC_CONTROL_SERVICE_APP, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)

class service_control_task : public task
{
public:
    service_control_task(service_node *node, bool start, bool cleanup = false)
        : task(LPC_CONTROL_SERVICE_APP, 0, node), _node(node), _start(start), _cleanup(cleanup)
    {
    }

    void exec()
    {
        auto &sp = _node->spec();

        if (_start) {
            error_code err;
            err = _node->start_app();
            CHECK_EQ_MSG(err, ERR_OK, "start app failed");
        } else {
            LOG_INFO("stop app result({})", _node->stop_app(_cleanup));
        }
    }

private:
    service_node *_node;
    bool _start;   // false for stop
    bool _cleanup; // for stop
};

namespace tools {

tool_base::tool_base(const char *name) { _name = name; }

toollet::toollet(const char *name) : tool_base(name) {}

tool_app::tool_app(const char *name) : tool_base(name) {}

void tool_app::start_all_apps()
{
    const auto &apps = service_engine::instance().get_all_nodes();
    for (const auto &kv : apps) {
        task *t = new service_control_task(kv.second.get(), true);
        t->set_delay(1000 * kv.second.get()->spec().delay_seconds);
        t->enqueue();
    }
}

void tool_app::stop_all_apps(bool cleanup)
{
    const auto &apps = service_engine::instance().get_all_nodes();
    for (const auto &kv : apps) {
        task *t = new service_control_task(kv.second.get(), false, cleanup);
        t->enqueue();
    }
}

const service_spec &tool_app::get_service_spec() { return service_engine::instance().spec(); }

const service_spec &spec() { return service_engine::instance().spec(); }

const char *get_service_node_name(service_node *node) { return node->full_name(); }

join_point<void> sys_init_before_app_created("system.init.1");
join_point<void> sys_init_after_app_created("system.init.2");
join_point<void, sys_exit_type> sys_exit("system.exit");

namespace internal_use_only {
bool register_toollet(const char *name, toollet::factory f, ::dsn::provider_type type)
{
    return dsn::utils::factory_store<toollet>::register_factory(name, f, type);
}

bool register_tool(const char *name, tool_app::factory f, ::dsn::provider_type type)
{
    return dsn::utils::factory_store<tool_app>::register_factory(name, f, type);
}

bool register_component_provider(const char *name,
                                 timer_service::factory f,
                                 ::dsn::provider_type type)
{
    return dsn::utils::factory_store<timer_service>::register_factory(name, f, type);
}

bool register_component_provider(const char *name, task_queue::factory f, ::dsn::provider_type type)
{
    return dsn::utils::factory_store<task_queue>::register_factory(name, f, type);
}

bool register_component_provider(const char *name,
                                 task_worker::factory f,
                                 ::dsn::provider_type type)
{
    return dsn::utils::factory_store<task_worker>::register_factory(name, f, type);
}

bool register_component_provider(const char *name, network::factory f, ::dsn::provider_type type)
{
    return dsn::utils::factory_store<network>::register_factory(name, f, type);
}

bool register_component_provider(const char *name,
                                 env_provider::factory f,
                                 ::dsn::provider_type type)
{
    return dsn::utils::factory_store<env_provider>::register_factory(name, f, type);
}

bool register_component_provider(network_header_format fmt,
                                 const std::vector<const char *> &signatures,
                                 message_parser::factory f,
                                 size_t sz)
{
    message_parser_manager::instance().register_factory(fmt, signatures, f, sz);
    return true;
}

toollet *get_toollet(const char *name, ::dsn::provider_type type)
{
    toollet *tlt = nullptr;
    if (utils::singleton_store<std::string, toollet *>::instance().get(name, tlt))
        return tlt;
    else {
        tlt = utils::factory_store<toollet>::create(name, type, name);
        utils::singleton_store<std::string, toollet *>::instance().put(name, tlt);
        return tlt;
    }
}
} // namespace internal_use_only
} // namespace tools
} // namespace dsn
