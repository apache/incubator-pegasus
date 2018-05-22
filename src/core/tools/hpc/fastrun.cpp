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

#include <dsn/tool/fastrun.h>
#include "mix_all_io_looper.h"

namespace dsn {
namespace tools {
void fastrun::install(service_spec &spec)
{
    bool use_mixed_queue = false;
    if (spec.disk_io_mode == IOE_PER_QUEUE || spec.rpc_io_mode == IOE_PER_QUEUE ||
        spec.nfs_io_mode == IOE_PER_QUEUE || spec.timer_io_mode == IOE_PER_QUEUE)
        use_mixed_queue = true;

    if (spec.aio_factory_name == "") {
        spec.aio_factory_name = ("dsn::tools::hpc_aio_provider");
    }

    if (spec.env_factory_name == "")
        spec.env_factory_name = ("dsn::tools::hpc_env_provider");

    if (spec.timer_factory_name == "") {
        if (use_mixed_queue)
            spec.timer_factory_name = "dsn::tools::io_looper_timer_service";
        else
            spec.timer_factory_name = "dsn::tools::simple_timer_service";
    }

    {
        network_client_config cs;
        cs.factory_name = "dsn::tools::hpc_network_provider";
        cs.message_buffer_block_size = 1024 * 64;
        spec.network_default_client_cfs[RPC_CHANNEL_TCP] = cs;
    }
    {
        network_server_config cs2;
        cs2.port = 0;
        cs2.channel = RPC_CHANNEL_TCP;
        cs2.factory_name = "dsn::tools::hpc_network_provider";
        cs2.message_buffer_block_size = 1024 * 64;
        spec.network_default_server_cfs[cs2] = cs2;
    }

    {
        network_client_config cs;
        cs.factory_name = "dsn::tools::asio_udp_provider";
        cs.message_buffer_block_size = 1024 * 64;
        spec.network_default_client_cfs[RPC_CHANNEL_UDP] = cs;
    }
    {

        network_server_config cs2;
        cs2.port = 0;
        cs2.channel = RPC_CHANNEL_UDP;
        cs2.factory_name = "dsn::tools::asio_udp_provider";
        cs2.message_buffer_block_size = 1024 * 64;
        spec.network_default_server_cfs[cs2] = cs2;
    }

    if (spec.perf_counter_factory_name == "")
        spec.perf_counter_factory_name = "dsn::tools::simple_perf_counter";

    if (spec.logging_factory_name == "")
        spec.logging_factory_name = "dsn::tools::hpc_logger";

    if (spec.lock_factory_name == "")
        spec.lock_factory_name = ("dsn::tools::std_lock_provider");

    if (spec.lock_nr_factory_name == "")
        spec.lock_nr_factory_name = ("dsn::tools::std_lock_nr_provider");

    if (spec.rwlock_nr_factory_name == "")
        spec.rwlock_nr_factory_name = ("dsn::tools::std_rwlock_nr_provider");

    if (spec.semaphore_factory_name == "")
        spec.semaphore_factory_name = ("dsn::tools::std_semaphore_provider");

    if (spec.nfs_factory_name == "")
        spec.nfs_factory_name = "dsn::service::nfs_node_simple";

    for (auto it = spec.threadpool_specs.begin(); it != spec.threadpool_specs.end(); ++it) {
        threadpool_spec &tspec = *it;

        if (tspec.worker_factory_name == "")
            tspec.worker_factory_name =
                use_mixed_queue ? ("dsn::tools::io_looper_task_worker") : "dsn::task_worker";

        if (tspec.queue_factory_name == "")
            tspec.queue_factory_name = use_mixed_queue ? ("dsn::tools::io_looper_task_queue")
                                                       : "dsn::tools::hpc_concurrent_task_queue";
    }
}

void fastrun::run() { tool_app::run(); }
}
} // end namespace dsn::tools
