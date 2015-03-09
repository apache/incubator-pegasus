/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include <rdsn/tool/simulator.h>
#include "scheduler.h"
#include <rdsn/tool/providers.common.h>

#include "diske.sim.h"
#include "env.sim.h"
#include "network.sim.h"
#include "task_engine.sim.h"

namespace rdsn { namespace tools {
    
void simulator::install(service_spec& spec)
{
    register_common_providers();
    
    register_component_provider<sim_aio_provider>("rdsn::tools::sim_aio_provider");
    register_component_provider<sim_env_provider>("rdsn::tools::sim_env_provider");
    register_component_provider<sim_network_provider>("rdsn::tools::sim_network_provider");
    register_component_provider<sim_task_queue>("rdsn::tools::sim_task_queue");
    register_component_provider<sim_semaphore_provider>("rdsn::tools::sim_semaphore_provider");

    scheduler::instance();

    if (spec.aio_factory_name == "")
        spec.aio_factory_name = ("rdsn::tools::sim_aio_provider");

    if (spec.env_factory_name == "")
        spec.env_factory_name = ("rdsn::tools::sim_env_provider");

    if (spec.network_factory_names[RPC_CHANNEL_TCP] == "")
        spec.network_factory_names[RPC_CHANNEL_TCP] = ("rdsn::tools::sim_network_provider");

    if (spec.network_factory_names[RPC_CHANNEL_UDP] == "")
        spec.network_factory_names[RPC_CHANNEL_UDP] = ("rdsn::tools::sim_network_provider");

    if (spec.perf_counter_factory_name == "")
        spec.perf_counter_factory_name = "rdsn::tools::wrong_perf_counter";
    
    if (spec.logging_factory_name == "")
        spec.logging_factory_name = "rdsn::tools::screen_logger";

    if (spec.lock_factory_name == "")
        spec.lock_factory_name = ("rdsn::tools::std_lock_provider");

    if (spec.rwlock_factory_name == "")
        spec.rwlock_factory_name = ("rdsn::tools::std_rwlock_provider");

    if (spec.semaphore_factory_name == "")
        spec.semaphore_factory_name = ("rdsn::tools::sim_semaphore_provider");

    for (auto it = spec.threadpool_specs.begin(); it != spec.threadpool_specs.end(); it++)
    {
        threadpool_spec& tspec = *it;

        if (tspec.worker_factory_name == "")
            tspec.worker_factory_name = ("rdsn::task_worker");

        if (tspec.queue_factory_name == "")
            tspec.queue_factory_name = ("rdsn::tools::sim_task_queue");
    }

}

void simulator::run()
{
    scheduler::instance().start();
    tool_app::run();
}

}} // end namespace rdsn::tools
