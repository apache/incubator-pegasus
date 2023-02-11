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

#include "distributed_lock_service_simple.h"
#include "greedy_load_balancer.h"
#include "http/http_server.h"
#include "http/service_version.h"
#include "meta/meta_service_app.h"
#include "meta/meta_state_service.h"
#include "meta/partition_guardian.h"
#include "meta_http_service.h"
#include "meta_service.h"
#include "meta_state_service_simple.h"
#include "meta_state_service_zookeeper.h"
#include "runtime/service_app.h"
#include "server_load_balancer.h"
#include "utils/distributed_lock_service.h"
#include "utils/error_code.h"
#include "utils/factory_store.h"
#include "zookeeper/distributed_lock_service_zookeeper.h"

namespace dsn {
namespace service {

#define register_component(name, base_type, derived_type)                                          \
    do {                                                                                           \
        utils::factory_store<base_type>::register_factory(                                         \
            name, base_type::create<derived_type>, PROVIDER_TYPE_MAIN);                            \
    } while (0)

void meta_service_app::register_components()
{
    register_component("distributed_lock_service_simple",
                       dist::distributed_lock_service,
                       dist::distributed_lock_service_simple);

    register_component("distributed_lock_service_zookeeper",
                       dist::distributed_lock_service,
                       dist::distributed_lock_service_zookeeper);

    register_component(
        "meta_state_service_simple", dist::meta_state_service, dist::meta_state_service_simple);
    register_component("meta_state_service_zookeeper",
                       dist::meta_state_service,
                       dist::meta_state_service_zookeeper);

    register_component("greedy_load_balancer",
                       replication::server_load_balancer,
                       replication::greedy_load_balancer);
    register_component(
        "partition_guardian", replication::partition_guardian, replication::partition_guardian);
}

void meta_service_app::register_all()
{
    dsn::service_app::register_factory<meta_service_app>("meta");
    register_components();
}

meta_service_app::meta_service_app(const service_app_info *info) : service_app(info)
{
    // create in constructor because it may be used in checker before started
    _service.reset(new replication::meta_service());

    // add http service
    register_http_service(new replication::meta_http_service(_service.get()));
    start_http_server();
}

meta_service_app::~meta_service_app() {}

error_code meta_service_app::start(const std::vector<std::string> &args)
{
    if (args.size() >= 2) {
        app_version.version = *(args.end() - 2);
        app_version.git_commit = *(args.end() - 1);
    }
    return _service->start();
}

error_code meta_service_app::stop(bool /*cleanup*/)
{
    _service->stop();
    return ERR_OK;
}
} // namespace service
} // namespace dsn
