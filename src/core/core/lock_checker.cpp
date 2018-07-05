/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/task.h>
#include <dsn/tool_api.h>
#include "task_engine.h"

namespace dsn {
namespace lock_checker {
__thread int zlock_exclusive_count = 0;
__thread int zlock_shared_count = 0;

void check_wait_safety()
{
    if (zlock_exclusive_count + zlock_shared_count > 0) {
        dwarn("wait inside locks may lead to deadlocks - current thread owns %u exclusive locks "
              "and %u shared locks now.",
              zlock_exclusive_count,
              zlock_shared_count);
    }
}

void check_dangling_lock()
{
    if (zlock_exclusive_count + zlock_shared_count > 0) {
        dwarn("locks should not be hold at this point - current thread owns %u exclusive locks and "
              "%u shared locks now.",
              zlock_exclusive_count,
              zlock_shared_count);
    }
}

void check_wait_task(task *waitee)
{
    check_wait_safety();

    // not in worker thread
    if (task::get_current_worker() == nullptr)
        return;

    // caller and callee don't share the same thread pool,
    if (waitee->spec().type != TASK_TYPE_RPC_RESPONSE &&
        (waitee->spec().pool_code != task::get_current_worker()->pool_spec().pool_code))
        return;

    // callee is empty
    if (waitee->is_empty())
        return;

    // there are enough concurrency
    if (!task::get_current_worker()->pool_spec().partitioned &&
        task::get_current_worker()->pool_spec().worker_count > 1)
        return;

    dwarn("task %s waits for another task %s sharing the same thread pool "
          "- will lead to deadlocks easily (e.g., when worker_count = 1 or when the pool "
          "is partitioned)",
          task::get_current_task()->spec().code.to_string(),
          waitee->spec().code.to_string());
}
}
}
