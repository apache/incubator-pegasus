
# include <dsn/internal/task.h>

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

            if (nullptr != task::get_current_task() && !waitee->is_empty())
            {
                if (TASK_TYPE_RPC_RESPONSE == waitee->spec().type ||
                    task::get_current_task()->spec().pool_code == waitee->spec().pool_code)
                {
                    dassert(false, "task %s waits for another task %s sharing the same thread pool - will lead to deadlocks easily (e.g., when worker_count = 1 or when the pool is partitioned)",
                        dsn_task_code_to_string(task::get_current_task()->spec().code),
                        dsn_task_code_to_string(waitee->spec().code)
                        );
                }
            }
        }
    }
}