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

#include "task_queue.h"

#include <stdio.h>

#include "runtime/rpc/network.h"
#include "runtime/rpc/rpc_engine.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/service_engine.h"
#include "runtime/task/task.h"
#include "runtime/task/task_spec.h"
#include "task_engine.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/exp_delay.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_spec.h"

namespace dsn {

task_queue::task_queue(task_worker_pool *pool, int index, task_queue *inner_provider)
    : _pool(pool), _queue_length(0)
{
    char num[30];
    sprintf(num, "%u", index);
    _index = index;
    _name = pool->spec().name + '.';
    _name.append(num);
    _queue_length_counter.init_global_counter(_pool->node()->full_name(),
                                              "engine",
                                              (_name + ".queue.length").c_str(),
                                              COUNTER_TYPE_NUMBER,
                                              "task queue length");
    _delay_task_counter.init_global_counter(_pool->node()->full_name(),
                                            "engine",
                                            (_name + ".queue.delay_task").c_str(),
                                            COUNTER_TYPE_VOLATILE_NUMBER,
                                            "delay count of tasks before enqueue");
    _reject_task_counter.init_global_counter(_pool->node()->full_name(),
                                             "engine",
                                             (_name + ".queue.reject_task").c_str(),
                                             COUNTER_TYPE_VOLATILE_NUMBER,
                                             "reject count of tasks before enqueue");
    _virtual_queue_length = 0;
    _spec = (threadpool_spec *)&pool->spec();
}

task_queue::~task_queue() = default;

// This function is used to throttle tasks before they enter the queue
// `queue_length_throttling_threshold` is configured by task pool
// `throttling_mode` is configured by the specific task
// Because not all tasks in the queue can handle the `ERR_BUSY` exception
void task_queue::enqueue_internal(task *task)
{
    auto &sp = task->spec();
    auto throttle_mode = sp.rpc_request_throttling_mode;
    if (throttle_mode != TM_NONE) {
        int ac_value = 0;
        if (_spec->enable_virtual_queue_throttling) {
            ac_value = _virtual_queue_length;
        } else {
            ac_value = count();
        }

        if (throttle_mode == TM_DELAY) {
            int delay_ms =
                sp.rpc_request_delayer.delay(ac_value, _spec->queue_length_throttling_threshold);
            if (delay_ms > 0) {
                auto rtask = static_cast<rpc_request_task *>(task);
                if (rtask->get_request()->io_session->delay_recv(delay_ms)) {
                    _delay_task_counter->increment();
                }
            }
        } else {
            DCHECK_EQ_MSG(TM_REJECT, throttle_mode, "unknow mode {}", throttle_mode);

            if (ac_value > _spec->queue_length_throttling_threshold) {
                auto rtask = static_cast<rpc_request_task *>(task);
                auto resp = rtask->get_request()->create_response();
                task::get_current_rpc()->reply(resp, ERR_BUSY);
                _reject_task_counter->increment();
                task->release_ref(); // added in task::enqueue(pool)
                return;
            }
        }
    }

    tls_dsn.last_worker_queue_size = increase_count();
    enqueue(task);
}
} // namespace dsn
