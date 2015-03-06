#include "diske.sim.h"
#include <rdsn/service_api.h>

#define __TITLE__ "aio_provider"

namespace rdsn { namespace tools {

DEFINE_TASK_CODE(LPC_NATIVE_AIO_REDIRECT, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)

sim_aio_provider::sim_aio_provider(disk_engine* disk, aio_provider* inner_provider)
: NATIVE_AIO_PROVIDER(disk, inner_provider)
{
}

sim_aio_provider::~sim_aio_provider(void)
{
}

void sim_aio_provider::aio(aio_task_ptr& aio)
{
    error_code err;
    uint32_t bytes;

    err = aio_internal(aio, false, &bytes);
    complete_io(aio, err, bytes, 0);
}

}} // end namespace
