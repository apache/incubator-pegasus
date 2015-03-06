#include "env.sim.h"
#include "scheduler.h"

#define __TITLE__ "env.provider.simulator"

namespace rdsn { namespace tools {

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

    if (tool_app::config()->get_value<bool>("tools.simulator", "use_given_random_seed", false))
    {
        _seed = tool_app::config()->get_value<int>("tools.simulator", "random_seed", std::rand());
    }
    else
    {
        _seed = (int)get_current_physical_time_ns();
    }

    printf("simulation.random_seed.random seed for this round is %d\n", _seed);
    printf("\n----- Random seed for this round is %d\n", _seed);
}

}} // end namespace
