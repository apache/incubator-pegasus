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

#include "runtime/simulator.h"

#include <map>

#include "env.sim.h"
#include "runtime/global_config.h"
#include "runtime/task/task_engine.sim.h"
#include "runtime/task/task_spec.h"
#include "scheduler.h"
#include "service_engine.h"
#include "sim_clock.h"
#include "utils/clock.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/threadpool_spec.h"
#include "utils/zlock_provider.h"

namespace dsn {
namespace tools {

DSN_DECLARE_int32(random_seed);

/*static*/
void simulator::register_checker(const std::string &name, checker::factory f)
{
    scheduler::instance().add_checker(name, f);
}

void simulator::install(service_spec &spec)
{
    register_component_provider<sim_env_provider>("dsn::tools::sim_env_provider");
    register_component_provider<sim_task_queue>("dsn::tools::sim_task_queue");
    register_component_provider<sim_timer_service>("dsn::tools::sim_timer_service");

    semaphore_provider::register_component<sim_semaphore_provider>(
        "dsn::tools::sim_semaphore_provider");
    lock_provider::register_component<sim_lock_provider>("dsn::tools::sim_lock_provider");
    lock_nr_provider::register_component<sim_lock_nr_provider>("dsn::tools::sim_lock_nr_provider");
    rwlock_nr_provider::register_component<sim_rwlock_nr_provider>(
        "dsn::tools::sim_rwlock_nr_provider");

    scheduler::instance();

    if (spec.env_factory_name == "")
        spec.env_factory_name = ("dsn::tools::sim_env_provider");

    if (spec.timer_factory_name == "")
        spec.timer_factory_name = ("dsn::tools::sim_timer_service");

    network_client_config cs;
    cs.factory_name = "dsn::tools::sim_network_provider";
    cs.message_buffer_block_size = 1024 * 64;
    spec.network_default_client_cfs[RPC_CHANNEL_TCP] = cs;
    spec.network_default_client_cfs[RPC_CHANNEL_UDP] = cs;

    network_server_config cs2;
    cs2.port = 0;
    cs2.factory_name = "dsn::tools::sim_network_provider";
    cs2.message_buffer_block_size = 1024 * 64;
    cs2.channel = RPC_CHANNEL_TCP;
    spec.network_default_server_cfs[cs2] = cs2;
    cs2.channel = RPC_CHANNEL_UDP;
    spec.network_default_server_cfs[cs2] = cs2;

    if (spec.logging_factory_name == "")
        spec.logging_factory_name = "dsn::tools::simple_logger";

    if (spec.lock_factory_name == "")
        spec.lock_factory_name = ("dsn::tools::sim_lock_provider");

    if (spec.lock_nr_factory_name == "")
        spec.lock_nr_factory_name = ("dsn::tools::sim_lock_nr_provider");

    if (spec.rwlock_nr_factory_name == "")
        spec.rwlock_nr_factory_name = ("dsn::tools::sim_rwlock_nr_provider");

    if (spec.semaphore_factory_name == "")
        spec.semaphore_factory_name = ("dsn::tools::sim_semaphore_provider");

    for (auto it = spec.threadpool_specs.begin(); it != spec.threadpool_specs.end(); ++it) {
        threadpool_spec &tspec = *it;

        if (tspec.worker_factory_name == "")
            tspec.worker_factory_name = ("dsn::task_worker");

        if (tspec.queue_factory_name == "")
            tspec.queue_factory_name = ("dsn::tools::sim_task_queue");
    }

    sys_exit.put_front(simulator::on_system_exit, "simulator");

    // the new sim_clock is taken over by unique_ptr in clock instance
    utils::clock::instance()->mock(new sim_clock());

    service_engine::instance().set_simulator();
}

void simulator::on_system_exit(sys_exit_type st)
{
    LOG_INFO("system exits, you can replay this process using random seed {}", FLAGS_random_seed);
}

void simulator::run()
{
    scheduler::instance().start();
    tool_app::run();
}
} // namespace tools
} // namespace dsn
