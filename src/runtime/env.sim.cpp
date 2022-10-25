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

#include "env.sim.h"
#include "scheduler.h"

#include "utils/rand.h"

namespace dsn {
namespace tools {

/*static*/ int sim_env_provider::_seed;

void sim_env_provider::on_worker_start(task_worker *worker)
{
    rand::reseed_thread_local_rng(
        (_seed + worker->index() + worker->index() * worker->pool_spec().pool_code) ^
        worker->index());
}

sim_env_provider::sim_env_provider(env_provider *inner_provider) : env_provider(inner_provider)
{
    task_worker::on_start.put_front(on_worker_start, "sim_env_provider::on_worker_start");

    _seed =
        (int)dsn_config_get_value_uint64("tools.simulator",
                                         "random_seed",
                                         0,
                                         "random seed for the simulator, 0 for random random seed");
    if (_seed == 0) {
        _seed = std::random_device{}();
    }

    LOG_ERROR("simulation.random seed for this round is %d", _seed);
}

} // namespace tools
} // namespace dsn
