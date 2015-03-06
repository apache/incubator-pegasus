
# include "net_provider.h"
# include <rdsn/tool/providers.common.h>
# include "lockp.std.h"
# include "logger.screen.h"
# include "native_aio_provider.win.h"
# include "native_aio_provider.posix.h"
# include "wrong_perf_counter.h"
# include "simple_task_queue.h"

namespace rdsn {
    namespace tools {
        void register_common_providers()
        {
            register_component_provider<env_provider>("rdsn::env_provider");
            register_component_provider<task_worker>("rdsn::task_worker");
            register_component_provider<screen_logger>("rdsn::tools::screen_logger");
            register_component_provider<std_lock_provider>("rdsn::tools::std_lock_provider");
            register_component_provider<std_rwlock_provider>("rdsn::tools::std_rwlock_provider");
            register_component_provider<std_semaphore_provider>("rdsn::tools::std_semaphore_provider");
            register_component_provider<wrong_perf_counter>("rdsn::tools::wrong_perf_counter");
            register_component_provider<asio_network_provider>("rdsn::tools::asio_network_provider");
            register_component_provider<simple_task_queue>("rdsn::tools::simple_task_queue");
#if defined(_WIN32)
            register_component_provider<native_win_aio_provider>("rdsn::tools::native_aio_provider");
#else
            register_component_provider<native_posix_aio_provider>("rdsn::tools::native_aio_provider");
#endif
        }
    }
}
