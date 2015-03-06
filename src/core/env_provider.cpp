# include <rdsn/internal/env_provider.h>
# include <rdsn/internal/utils.h>
# include <chrono>

namespace rdsn {

//------------ env_provider ---------------

env_provider::env_provider(env_provider* inner_provider)
{
}
    
uint64_t env_provider::random64(uint64_t min, uint64_t max)
{
    uint64_t gap = max - min + 1;
    if (gap == 0)
    {
        /*uint64_t a,b,c,d;*/
        return utils::get_random64();
    }
    else if (gap == (uint64_t)RAND_MAX + 1)
    {
        return (uint64_t)std::rand();
    }
    else
    {
        gap = static_cast<uint64_t>(static_cast<double>(97 * gap) * (double)std::rand() / (double)RAND_MAX);
        gap = gap % (max - min + 1);
        return min + gap;
    }
}

/*static*/ uint64_t env_provider::get_current_physical_time_ns()
{
    auto now = std::chrono::high_resolution_clock::now();
    auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return nanos;
}

} // end namespace
