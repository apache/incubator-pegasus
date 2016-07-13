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

# include <dsn/dist/replication.h>
# include <dsn/utility/factory_store.h>
# include <dsn/dist/replication/meta_service_app.h>

# include "distributed_lock_service_simple.h"
# include "meta_state_service_simple.h"

# include "../zookeeper/distributed_lock_service_zookeeper.h"
# include "../zookeeper/meta_state_service_zookeeper.h"

# include "server_load_balancer.h"
# include "greedy_load_balancer.h"

# include "meta_service.h"

# ifdef DSN_META_SERVER_DYNAMIC_LIB

# include <dsn/utility/module_init.cpp.h>

MODULE_INIT_BEGIN(meta)
    dsn::register_app< ::dsn::service::meta_service_app>("meta");
MODULE_INIT_END

# endif

namespace dsn {
    namespace service {
        static bool register_component_provider(
            const char* name,
            dist::distributed_lock_service::factory f)
        {
            return utils::factory_store<dist::distributed_lock_service>::register_factory(
                name,
                f,
                PROVIDER_TYPE_MAIN);
        }

        static bool register_component_provider(
            const char* name,
            dist::meta_state_service::factory f)
        {
            return utils::factory_store<dist::meta_state_service>::register_factory(
                name,
                f,
                PROVIDER_TYPE_MAIN);
        }

        static bool register_component_provider(
            const char* name,
            replication::server_load_balancer::factory f)
        {
            return utils::factory_store<replication::server_load_balancer>::register_factory(
                name,
                f,
                PROVIDER_TYPE_MAIN);
        }

        meta_service_app::meta_service_app(dsn_gpid gpid): service_app(gpid)
        {
            // create in constructor because it may be used in checker before started
            _service.reset(new replication::meta_service());

            register_component_provider("distributed_lock_service_simple", dist::distributed_lock_service::create<dist::distributed_lock_service_simple>);
            register_component_provider("meta_state_service_simple", dist::meta_state_service::create<dist::meta_state_service_simple>);

            register_component_provider("distributed_lock_service_zookeeper", dist::distributed_lock_service::create<dist::distributed_lock_service_zookeeper>);
            register_component_provider("meta_state_service_zookeeper", dist::meta_state_service::create<dist::meta_state_service_zookeeper>);

            register_component_provider("simple_load_balancer", replication::server_load_balancer::create<replication::simple_load_balancer>);
            register_component_provider("greedy_load_balancer", replication::server_load_balancer::create<replication::greedy_load_balancer>);
            /////////////////////////////////////////////////////
            //// register more provides here used by meta servers
            /////////////////////////////////////////////////////
        }

        meta_service_app::~meta_service_app()
        {
        }

        error_code meta_service_app::start(int argc, char** argv)
        {
            //TODO: handle the load & restore
            return _service->start();
        }

        error_code meta_service_app::stop(bool /*cleanup*/)
        {
            _service.reset(nullptr);
            return ERR_OK;
        }
    }
}
