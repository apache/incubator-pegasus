#pragma once

#include <rdsn/tool_api.h>
#if defined(_WIN32)
#define NATIVE_AIO_PROVIDER native_win_aio_provider
#include "../common/native_aio_provider.win.h"
#else
#define NATIVE_AIO_PROVIDER native_posix_aio_provider
#include "../common/native_aio_provider.posix.h"
#endif

namespace rdsn { namespace tools {

class sim_aio_provider : public NATIVE_AIO_PROVIDER
{
public:
    sim_aio_provider(disk_engine* disk, aio_provider* inner_provider);
    ~sim_aio_provider(void);

    virtual void    aio(aio_task_ptr& aio);
};

}} // end namespace
