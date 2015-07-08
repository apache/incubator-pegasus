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
#include "scheduler.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "env.provider.simulator"

namespace dsn { namespace tools {

/*static*/ int sim_env_provider::_seed;

uint64_t sim_env_provider::now_ns() const
{
    return scheduler::instance().now_ns();
}

uint64_t sim_env_provider::random64(uint64_t min, uint64_t max)
{
    uint64_t gap = max - min + 1;
    if (gap == 1) return min;

    uint64_t v = ((uint64_t)std::rand());
    v *= ((uint64_t)std::rand());
    v *= ((uint64_t)std::rand());
    v *= ((uint64_t)std::rand());
    v *= ((uint64_t)std::rand());
    v ^= ((uint64_t)std::rand());
    return gap == 0 ? (min + v) : (min + v % gap);
}

void sim_env_provider::on_worker_start(task_worker* worker)
{
    std::srand((_seed + worker->index() + worker->index()*worker->pool_spec().pool_code) ^ worker->index());
}

sim_env_provider::sim_env_provider(env_provider* inner_provider)
    : env_provider(inner_provider)
{
    task_worker::on_start.put_front(on_worker_start, "sim_env_provider::on_worker_start");

    _seed = config()->get_value<int>("tools.simulator", "random_seed", 0);
    if (_seed == 0)
    {
        _seed = static_cast<int>(get_current_physical_time_ns())  * std::rand();
    }

    derror("simulation.random seed for this round is %d", _seed);
}

}} // end namespace
