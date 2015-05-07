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
#include <dsn/tool/simulator.h>
#include "scheduler.h"
#include <dsn/tool/providers.common.h>

#include "diske.sim.h"
#include "env.sim.h"
#include "task_engine.sim.h"

namespace dsn { namespace tools {
    
void simulator::install(service_spec& spec)
{
    register_common_providers();
    
    register_component_provider<sim_aio_provider>("dsn::tools::sim_aio_provider");
    register_component_provider<sim_env_provider>("dsn::tools::sim_env_provider");
    register_component_provider<sim_task_queue>("dsn::tools::sim_task_queue");
    register_component_provider<sim_semaphore_provider>("dsn::tools::sim_semaphore_provider");

    scheduler::instance();

    if (spec.aio_factory_name == "")
        spec.aio_factory_name = ("dsn::tools::sim_aio_provider");

    if (spec.env_factory_name == "")
        spec.env_factory_name = ("dsn::tools::sim_env_provider");

    network_config_spec cs;
    cs.channel = RPC_CHANNEL_TCP;
    cs.factory_name = "dsn::tools::sim_network_provider";
    cs.message_format = "dsn";
    cs.message_buffer_block_size = 1024 * 64;
    spec.register_network(cs, false);

    cs.channel = RPC_CHANNEL_UDP;
    spec.register_network(cs, false);

    if (spec.perf_counter_factory_name == "")
        spec.perf_counter_factory_name = "dsn::tools::simple_perf_counter";
    
    if (spec.logging_factory_name == "")
        spec.logging_factory_name = "dsn::tools::simple_logger";

    if (spec.lock_factory_name == "")
        spec.lock_factory_name = ("dsn::tools::std_lock_provider");

    if (spec.rwlock_factory_name == "")
        spec.rwlock_factory_name = ("dsn::tools::std_rwlock_provider");

    if (spec.semaphore_factory_name == "")
        spec.semaphore_factory_name = ("dsn::tools::sim_semaphore_provider");

    for (auto it = spec.threadpool_specs.begin(); it != spec.threadpool_specs.end(); it++)
    {
        threadpool_spec& tspec = *it;

        if (tspec.worker_factory_name == "")
            tspec.worker_factory_name = ("dsn::task_worker");

        if (tspec.queue_factory_name == "")
            tspec.queue_factory_name = ("dsn::tools::sim_task_queue");
    }
}

void simulator::run()
{
    scheduler::instance().start();
    tool_app::run();
}

}} // end namespace dsn::tools
