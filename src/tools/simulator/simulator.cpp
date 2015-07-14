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

# include <dsn/tool/simulator.h>
# include "scheduler.h"
# include <dsn/tool/providers.common.h>

# include "diske.sim.h"
# include "env.sim.h"
# include "task_engine.sim.h"
# include <dsn/internal/logging.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "tools.simulator"

namespace dsn { namespace tools {

void simulator::add_checker(checker* chker)
{
    scheduler::instance().add_checker(chker);
}

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

    network_client_config cs;
    cs.factory_name = "dsn::tools::sim_network_provider";
    cs.message_buffer_block_size = 1024 * 64;
    spec.network_default_client_cfs[RPC_CHANNEL_TCP] = cs;
    spec.network_default_client_cfs[RPC_CHANNEL_UDP] = cs;

    network_server_config cs2;
    cs2.port = 0;    
    cs2.hdr_format = NET_HDR_DSN;
    cs2.factory_name = "dsn::tools::sim_network_provider";
    cs2.message_buffer_block_size = 1024 * 64;
    cs2.channel = RPC_CHANNEL_TCP;
    spec.network_default_server_cfs[cs2] = cs2;
    cs2.channel = RPC_CHANNEL_UDP;
    spec.network_default_server_cfs[cs2] = cs2;

    if (spec.perf_counter_factory_name == "")
        spec.perf_counter_factory_name = "dsn::tools::simple_perf_counter";
    
    if (spec.logging_factory_name == "")
        spec.logging_factory_name = "dsn::tools::simple_logger";

    if (spec.memory_factory_name == "")
        spec.memory_factory_name = "dsn::default_memory_provider";

    if (spec.tools_memory_factory_name == "")
        spec.tools_memory_factory_name = "dsn::default_memory_provider";

    if (spec.lock_factory_name == "")
        spec.lock_factory_name = ("dsn::tools::std_lock_provider");

    if (spec.rwlock_nr_factory_name == "")
        spec.rwlock_nr_factory_name = ("dsn::tools::std_rwlock_nr_provider");

    if (spec.semaphore_factory_name == "")
        spec.semaphore_factory_name = ("dsn::tools::sim_semaphore_provider");

    if (spec.nfs_factory_name == "")
        spec.nfs_factory_name = "dsn::service::nfs_node_impl";

    for (auto it = spec.threadpool_specs.begin(); it != spec.threadpool_specs.end(); it++)
    {
        threadpool_spec& tspec = *it;

        if (tspec.worker_factory_name == "")
            tspec.worker_factory_name = ("dsn::task_worker");

        if (tspec.queue_factory_name == "")
            tspec.queue_factory_name = ("dsn::tools::sim_task_queue");
    }

    sys_exit.put_back(simulator::on_system_exit, "simulator");
}

void simulator::on_system_exit(sys_exit_type st)
{
    derror("system exits, you can replay this process using random seed %d",        
        sim_env_provider::seed()
        );
}

void simulator::run()
{
    scheduler::instance().start();
    tool_app::run();
}

}} // end namespace dsn::tools
