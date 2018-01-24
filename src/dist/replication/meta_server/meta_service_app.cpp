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

#include <dsn/dist/replication.h>
#include <dsn/utility/factory_store.h>
#include <dsn/dist/replication/meta_service_app.h>

#include "distributed_lock_service_simple.h"
#include "meta_state_service_simple.h"

#include "dist/replication/zookeeper/distributed_lock_service_zookeeper.h"
#include "dist/replication/zookeeper/meta_state_service_zookeeper.h"

#include "server_load_balancer.h"
#include "greedy_load_balancer.h"

#include "meta_service.h"

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "meta.service.app"

static bool register_component_provider(const char *name,
                                        dsn::dist::distributed_lock_service::factory f)
{
    return dsn::utils::factory_store<dsn::dist::distributed_lock_service>::register_factory(
        name, f, dsn::PROVIDER_TYPE_MAIN);
}

static bool register_component_provider(const char *name, dsn::dist::meta_state_service::factory f)
{
    return dsn::utils::factory_store<dsn::dist::meta_state_service>::register_factory(
        name, f, dsn::PROVIDER_TYPE_MAIN);
}

static bool register_component_provider(const char *name,
                                        dsn::replication::server_load_balancer::factory f)
{
    return dsn::utils::factory_store<dsn::replication::server_load_balancer>::register_factory(
        name, f, dsn::PROVIDER_TYPE_MAIN);
}

extern "C" {
void dsn_meta_sever_register_providers()
{
    register_component_provider(
        "distributed_lock_service_simple",
        dsn::dist::distributed_lock_service::create<dsn::dist::distributed_lock_service_simple>);
    register_component_provider(
        "meta_state_service_simple",
        dsn::dist::meta_state_service::create<dsn::dist::meta_state_service_simple>);

    register_component_provider(
        "distributed_lock_service_zookeeper",
        dsn::dist::distributed_lock_service::create<dsn::dist::distributed_lock_service_zookeeper>);
    register_component_provider(
        "meta_state_service_zookeeper",
        dsn::dist::meta_state_service::create<dsn::dist::meta_state_service_zookeeper>);

    register_component_provider(
        "simple_load_balancer",
        dsn::replication::server_load_balancer::create<dsn::replication::simple_load_balancer>);
    register_component_provider(
        "greedy_load_balancer",
        dsn::replication::server_load_balancer::create<dsn::replication::greedy_load_balancer>);
}

dsn_error_t dsn_meta_server_bridge(int argc, char **argv)
{
    dsn::service_app::register_factory<::dsn::service::meta_service_app>("meta");
    dsn_meta_sever_register_providers();
    return dsn::ERR_OK;
}
}

namespace dsn {
namespace service {

meta_service_app::meta_service_app(const service_app_info *info) : service_app(info)
{
    // create in constructor because it may be used in checker before started
    _service.reset(new replication::meta_service());
}

meta_service_app::~meta_service_app() {}

error_code meta_service_app::start(const std::vector<std::string> &args)
{
    // TODO: handle the load & restore
    return _service->start();
}

error_code meta_service_app::stop(bool /*cleanup*/)
{
    _service.reset(nullptr);
    return ERR_OK;
}
}
}
