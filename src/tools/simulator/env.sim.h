#pragma once

#include <rdsn/tool_api.h>

namespace rdsn { namespace tools {

class sim_env_provider : public env_provider
{
public:
    sim_env_provider(env_provider* inner_provider);

    // service local time (can be logical or physical)
    virtual uint64_t now_ns() const;
    virtual uint64_t random64(uint64_t min, uint64_t max);

private:
    static void on_worker_start(task_worker* worker);
    static int _seed;
};

}} // end namespace
