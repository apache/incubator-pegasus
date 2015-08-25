
# include <dsn/internal/task.h>
# include <dsn/tool_api.h>
# include "task_engine.h"

namespace dsn
{
    namespace lock_checker
    {
        __thread int zlock_exclusive_count = 0;
        __thread int zlock_shared_count = 0;

        void check_wait_safety()
        {
            if (zlock_exclusive_count + zlock_shared_count > 0)
            {
                dassert(false, "wait inside locks may lead to deadlocks - current thread owns %u exclusive locks and %u shared locks now.",
                    zlock_exclusive_count, zlock_shared_count
                    );
            }
        }

        void check_dangling_lock()
        {
            if (zlock_exclusive_count + zlock_shared_count > 0)
            {
                dassert(false, "locks should not be hold at this point - current thread owns %u exclusive locks and %u shared locks now.",
                    zlock_exclusive_count, zlock_shared_count
                    );
            }
        }

        void check_wait_task(task* waitee)
        {
            check_wait_safety();

            if (
                // in worker thread
                task::get_current_worker() != nullptr

                // caller and callee share the same thread pool,
                // or caller and io notification which notifies the callee share the same thread pool
                && (waitee->spec().type == TASK_TYPE_RPC_RESPONSE
                || (waitee->spec().pool_code == task::get_current_worker()->pool_spec().pool_code)
                || (
                    // assuming wait is with the same thread when the io call is emitted
                    // in this case io notificaiton is received by the current thread
                    ::dsn::tools::spec().io_mode == IOE_PER_QUEUE
                   )
                )

                // callee is not empty or we are using IOE_PER_QUEUE mode (io received by current thread)
                && (!waitee->is_empty()
                    || ::dsn::tools::spec().io_mode == IOE_PER_QUEUE
                )

                // not much concurrency in the current pool
                && (task::get_current_worker()->pool_spec().partitioned
                    || task::get_current_worker()->pool_spec().worker_count == 1)
                )
            {
                if (::dsn::tools::spec().io_mode == IOE_PER_QUEUE
                    && (task::get_current_worker()->pool_spec().partitioned
                        || task::get_current_worker()->pool_spec().worker_count == 1)
                        )
                {
                    dassert(false, 
                        "cannot call task wait in worker thread '%s' that also serves as io thread "
                        "(io_mode == IOE_PER_QUEUE) "
                        "when the thread pool is partitioned or the worker thread number is 1",
                        task::get_current_worker()->name().c_str()
                        );
                }
                else
                {
                    dassert(false, "task %s waits for another task %s sharing the same thread pool "
                        "- will lead to deadlocks easily (e.g., when worker_count = 1 or when the pool is partitioned)",
                        dsn_task_code_to_string(task::get_current_task()->spec().code),
                        dsn_task_code_to_string(waitee->spec().code)
                        );
                }
            }
        }
    }
}