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

#include "runtime/nativerun.h"

namespace dsn {
namespace tools {

void nativerun::install(service_spec &spec)
{
    if (spec.env_factory_name == "")
        spec.env_factory_name = ("dsn::env_provider");

    if (spec.timer_factory_name == "")
        spec.timer_factory_name = ("dsn::tools::simple_timer_service");
    {
        network_client_config cs;
        cs.factory_name = "dsn::tools::asio_network_provider";
        cs.message_buffer_block_size = 1024 * 64;
        spec.network_default_client_cfs[RPC_CHANNEL_TCP] = cs;
    }
    {
        network_server_config cs2;
        cs2.port = 0;
        cs2.channel = RPC_CHANNEL_TCP;
        cs2.factory_name = "dsn::tools::asio_network_provider";
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

    if (spec.logging_factory_name == "")
        spec.logging_factory_name = "dsn::tools::simple_logger";

    if (spec.lock_factory_name == "")
        spec.lock_factory_name = ("dsn::tools::std_lock_provider");

    if (spec.lock_nr_factory_name == "")
        spec.lock_nr_factory_name = ("dsn::tools::std_lock_nr_provider");

    if (spec.rwlock_nr_factory_name == "")
        spec.rwlock_nr_factory_name = ("dsn::tools::std_rwlock_nr_provider");

    if (spec.semaphore_factory_name == "")
        spec.semaphore_factory_name = ("dsn::tools::std_semaphore_provider");

    for (auto it = spec.threadpool_specs.begin(); it != spec.threadpool_specs.end(); ++it) {
        threadpool_spec &tspec = *it;

        if (tspec.worker_factory_name == "")
            tspec.worker_factory_name = ("dsn::task_worker");

        if (tspec.queue_factory_name == "")
            tspec.queue_factory_name = ("dsn::tools::simple_task_queue");
    }
}

void nativerun::run() { tool_app::run(); }
} // namespace tools
} // namespace dsn
