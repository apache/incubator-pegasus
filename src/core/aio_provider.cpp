# include <rdsn/internal/aio_provider.h>
# include "disk_engine.h"
# include <rdsn/internal/logging.h>

namespace rdsn {

aio_provider::aio_provider(disk_engine* disk, aio_provider* inner_provider)
    : _engine(disk)
{
}

void aio_provider::complete_io(aio_task_ptr& aio, error_code err, uint32_t bytes, int delay_milliseconds)
{
    _engine->complete_io(aio, err, bytes, delay_milliseconds);
}

} // end namespace rdsn
