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

#include <string_view>

#include "fmt/core.h"
#include "rpc/network.h"
#include "rpc/rpc_engine.h"
#include "rpc/rpc_message.h"
#include "task.h"
#include "task_engine.h"
#include "task_spec.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/exp_delay.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_spec.h"

METRIC_DEFINE_entity(queue);

METRIC_DEFINE_gauge_int64(queue,
                          queue_length,
                          dsn::metric_unit::kTasks,
                          "The length of task queue");

METRIC_DEFINE_counter(queue,
                      queue_delayed_tasks,
                      dsn::metric_unit::kTasks,
                      "The accumulative number of delayed tasks by throttling before enqueue");

METRIC_DEFINE_counter(queue,
                      queue_rejected_tasks,
                      dsn::metric_unit::kTasks,
                      "The accumulative number of rejected tasks by throttling before enqueue");

namespace dsn {

namespace {

metric_entity_ptr instantiate_queue_metric_entity(const std::string &queue_name)
{
    auto entity_id = fmt::format("queue@{}", queue_name);

    return METRIC_ENTITY_queue.instantiate(entity_id, {{"queue_name", queue_name}});
}

} // anonymous namespace

task_queue::task_queue(task_worker_pool *pool, int index, task_queue *inner_provider)
    : _pool(pool),
      _name(fmt::format("{}.{}", pool->spec().name, index)),
      _index(index),
      _queue_length(0),
      _spec(const_cast<threadpool_spec *>(&pool->spec())),
      _virtual_queue_length(0),
      _queue_metric_entity(instantiate_queue_metric_entity(_name)),
      METRIC_VAR_INIT_queue(queue_length),
      METRIC_VAR_INIT_queue(queue_delayed_tasks),
      METRIC_VAR_INIT_queue(queue_rejected_tasks)
{
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
                    METRIC_VAR_INCREMENT(queue_delayed_tasks);
                }
            }
        } else {
            DCHECK_EQ_MSG(TM_REJECT, throttle_mode, "unknow mode {}", throttle_mode);

            if (ac_value > _spec->queue_length_throttling_threshold) {
                auto rtask = static_cast<rpc_request_task *>(task);
                auto resp = rtask->get_request()->create_response();
                task::get_current_rpc()->reply(resp, ERR_BUSY);
                METRIC_VAR_INCREMENT(queue_rejected_tasks);
                task->release_ref(); // added in task::enqueue(pool)
                return;
            }
        }
    }

    tls_dsn.last_worker_queue_size = increase_count();
    enqueue(task);
}

const metric_entity_ptr &task_queue::queue_metric_entity() const
{
    CHECK_NOTNULL(_queue_metric_entity,
                  "queue metric entity (queue_name={}) should has been instantiated: "
                  "uninitialized entity cannot be used to instantiate metric",
                  _name);
    return _queue_metric_entity;
}

} // namespace dsn
