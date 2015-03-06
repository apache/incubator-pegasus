# pragma once

# include <rdsn/internal/rdsn_types.h>

namespace rdsn {

class env_provider
{
public:
    template <typename T> static env_provider* create(env_provider* inner_provider)
    {
        return new T(inner_provider);
    }

public:
    env_provider(env_provider* inner_provider);

    virtual uint64_t now_ns() const { return get_current_physical_time_ns(); }

    virtual uint64_t random64(uint64_t min, uint64_t max);

public:
    static uint64_t get_current_physical_time_ns();
};

} // end namespace
