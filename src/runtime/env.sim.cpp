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

#include "env.sim.h"

#include <random>

#include "task/task_worker.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/rand.h"
#include "utils/threadpool_code.h"
#include "utils/threadpool_spec.h"

DSN_DEFINE_int32(tools.simulator,
                 random_seed,
                 0,
                 "random seed for the simulator, 0 for random seed");

namespace dsn {
namespace tools {

void sim_env_provider::on_worker_start(task_worker *worker)
{
    rand::reseed_thread_local_rng(
        (FLAGS_random_seed + worker->index() + worker->index() * worker->pool_spec().pool_code) ^
        worker->index());
}

sim_env_provider::sim_env_provider(env_provider *inner_provider) : env_provider(inner_provider)
{
    task_worker::on_start.put_front(on_worker_start, "sim_env_provider::on_worker_start");

    if (FLAGS_random_seed == 0) {
        FLAGS_random_seed = std::random_device{}();
    }

    LOG_INFO("simulation.random seed for this round is {}", FLAGS_random_seed);
}

} // namespace tools
} // namespace dsn
