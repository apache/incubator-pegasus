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

#include "task.h"

#include <string.h>

#include "runtime/api_task.h"
#include "runtime/app_model.h"
#include "runtime/global_config.h"
#include "runtime/node_scoper.h"
#include "runtime/service_engine.h"
#include "runtime/task/task_spec.h"
#include "runtime/task/task_tracker.h"
#include "runtime/task/task_worker.h"
#include "task_engine.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/rand.h"
#include "utils/synchronize.h"
#include "utils/threadpool_code.h"
#include "utils/threadpool_spec.h"
#include "utils/utils.h"
#include "utils/zlocks.h"

namespace dsn {
__thread struct __tls_dsn__ tls_dsn;
__thread uint16_t tls_dsn_lower32_task_id_mask = 0;

/*static*/ void task::set_tls_dsn_context(service_node *node, // cannot be null
                                          task_worker *worker)
{
    memset(static_cast<void *>(&tls_dsn), 0, sizeof(tls_dsn));
    tls_dsn.magic = 0xdeadbeef;
    tls_dsn.worker_index = -1;

    if (node) {
        tls_dsn.node_id = node->id();

        if (worker != nullptr) {
            CHECK(worker->pool()->node() == node,
                  "worker not belonging to the given node: {} vs {}",
                  worker->pool()->node()->full_name(),
                  node->full_name());
        }

        tls_dsn.node = node;
        tls_dsn.worker = worker;
        tls_dsn.worker_index = worker ? worker->index() : -1;
        tls_dsn.current_task = nullptr;
        tls_dsn.rpc = node->rpc();
        tls_dsn.env = service_engine::instance().env();
    }

    tls_dsn.node_pool_thread_ids = (node ? ((uint64_t)(uint8_t)node->id()) : 0)
                                   << (64 - 8); // high 8 bits for node id
    tls_dsn.node_pool_thread_ids |=
        (worker ? ((uint64_t)(uint8_t)(int)worker->pool_spec().pool_code) : 0)
        << (64 - 8 - 8); // next 8 bits for pool id
    auto worker_idx = worker ? worker->index() : -1;
    if (worker_idx == -1) {
        worker_idx = utils::get_current_tid();
    }
    tls_dsn.node_pool_thread_ids |= ((uint64_t)(uint16_t)worker_idx)
                                    << 32; // next 16 bits for thread id
    tls_dsn.last_lower32_task_id = worker ? 0 : ((uint32_t)(++tls_dsn_lower32_task_id_mask)) << 16;
}

/*static*/ void task::on_tls_dsn_not_set()
{
    CHECK(service_engine::instance().spec().enable_default_app_mimic,
          "rDSN context is not initialized properly, to be fixed as follows:\n"
          "(1). the current thread does NOT belongs to any rDSN service node, please invoke "
          "dsn_mimic_app first,\n"
          "     or, you can enable [core] enable_default_app_mimic = true in your config "
          "file so mimic_app can be omitted\n"
          "(2). the current thread belongs to a rDSN service node, and you are writing "
          "providers for rDSN, please use\n"
          "     task::set_tls_dsn_context(...) at the beginning of your new thread in your "
          "providers;\n"
          "(3). this should not happen, please help fire an issue so we we can investigate");
    dsn_mimic_app("mimic", 1);
}

task::task(dsn::task_code code, int hash, service_node *node)
    : _state(TASK_STATE_READY), _wait_event(nullptr)
{
    _spec = task_spec::get(code);
    _hash = hash;
    _delay_milliseconds = 0;
    _wait_for_cancel = false;
    _is_null = false;
    next = nullptr;

    if (node != nullptr) {
        _node = node;
    } else {
        auto p = get_current_node();
        CHECK_NOTNULL(p,
                      "tasks without explicit service node can only be created "
                      "inside threads which is attached to specific node");
        _node = p;
    }

    if (tls_dsn.magic != 0xdeadbeef) {
        set_tls_dsn_context(nullptr, nullptr);
    }

    _task_id = tls_dsn.node_pool_thread_ids + (++tls_dsn.last_lower32_task_id);
}

task::~task()
{
    // ATTENTION: should do unset_tracker defore delete _wait_event
    _context_tracker.unset_tracker();

    if (nullptr != _wait_event.load()) {
        delete (utils::notify_event *)_wait_event.load();
        _wait_event.store(nullptr);
    }
}

bool task::set_retry(bool enqueue_immediately /*= true*/)
{
    task_state RUNNING_STATE = TASK_STATE_RUNNING;
    if (_state.compare_exchange_strong(
            RUNNING_STATE, TASK_STATE_READY, std::memory_order_relaxed)) {
        _error = enqueue_immediately ? ERR_OK : ERR_IO_PENDING;
        return true;
    } else
        return false;
}

void task::exec_internal()
{
    task_state READY_STATE = TASK_STATE_READY;
    task_state RUNNING_STATE = TASK_STATE_RUNNING;
    bool notify_if_necessary = true;

    if (_state.compare_exchange_strong(
            READY_STATE, TASK_STATE_RUNNING, std::memory_order_relaxed)) {
        CHECK_EQ_MSG(
            tls_dsn.magic, 0xdeadbeef, "thread is not inited with task::set_tls_dsn_context");

        task *parent_task = tls_dsn.current_task;
        tls_dsn.current_task = this;

        _spec->on_task_begin.execute(this);

        exec();

        // after exec(), one shot tasks are still in "running".
        // other tasks may call "set_retry" to reset tasks to "ready",
        // like timers and rpc_response_tasks
        if (_state.compare_exchange_strong(RUNNING_STATE,
                                           TASK_STATE_FINISHED,
                                           std::memory_order_release,
                                           std::memory_order_relaxed)) {
            _spec->on_task_end.execute(this);
            clear_non_trivial_on_task_end();
        } else {
            if (!_wait_for_cancel) {
                // for retried tasks such as timer or rpc_response_task
                notify_if_necessary = false;
                _spec->on_task_end.execute(this);

                if (ERR_OK == _error)
                    enqueue();
            } else {
                // for cancelled
                if (_state.compare_exchange_strong(READY_STATE,
                                                   TASK_STATE_CANCELLED,
                                                   std::memory_order_release,
                                                   std::memory_order_relaxed)) {
                    _spec->on_task_cancelled.execute(this);
                }

                // always call on_task_end()
                _spec->on_task_end.execute(this);

                // for timer task, we must call reset_callback after cancelled, because we don't
                // reset callback after exec()
                clear_non_trivial_on_task_end();
            }
        }

        tls_dsn.current_task = parent_task;
    }

    if (notify_if_necessary) {
        if (signal_waiters()) {
            spec().on_task_wait_notified.execute(this);
        }
    }

    if (!_spec->allow_inline && !_is_null) {
        lock_checker::check_dangling_lock();
    }

    this->release_ref(); // added in enqueue(pool)
}

bool task::signal_waiters()
{
    void *evt = _wait_event.load();
    if (evt != nullptr) {
        auto nevt = (utils::notify_event *)evt;
        nevt->notify();
        return true;
    }
    return false;
}

static void check_wait_task(task *waitee)
{
    lock_checker::check_wait_safety();

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

    LOG_WARNING("task {} waits for another task {} sharing the same thread pool "
                "- will lead to deadlocks easily (e.g., when worker_count = 1 or when the pool "
                "is partitioned)",
                task::get_current_task()->code(),
                waitee->code());
}

bool task::wait_on_cancel()
{
    check_wait_task(this);
    return wait(TIME_MS_MAX);
}

bool task::wait(int timeout_milliseconds)
{
    CHECK(this != task::get_current_task(), "task cannot wait itself");

    auto cs = state();

    if (cs >= TASK_STATE_FINISHED) {
        spec().on_task_wait_post.execute(get_current_task(), this, true);
        return true;
    }

    // TODO: using event pool instead
    void *evt = _wait_event.load();
    if (evt == nullptr) {
        evt = new utils::notify_event();

        void *null_h = nullptr;
        if (!_wait_event.compare_exchange_strong(null_h, evt)) {
            delete (utils::notify_event *)evt;
            evt = _wait_event.load();
        }
    }

    spec().on_task_wait_pre.execute(get_current_task(), this, (uint32_t)timeout_milliseconds);

    bool ret = (state() >= TASK_STATE_FINISHED);
    if (!ret) {
        auto nevt = (utils::notify_event *)evt;
        ret = (nevt->wait_for(timeout_milliseconds));
    }

    spec().on_task_wait_post.execute(get_current_task(), this, ret);
    return ret;
}

bool task::cancel(bool wait_until_finished, /*out*/ bool *finished /*= nullptr*/)
{
    task_state READY_STATE = TASK_STATE_READY;
    task *current_tsk = get_current_task();
    bool finish = false;
    bool succ = false;

    if (current_tsk != this) {
        if (_state.compare_exchange_strong(
                READY_STATE, TASK_STATE_CANCELLED, std::memory_order_relaxed)) {
            succ = true;
            finish = true;
        } else {
            task_state old_state = READY_STATE;
            if (old_state == TASK_STATE_CANCELLED) {
                succ = false; // this cancellation fails
                finish = true;
            } else if (old_state == TASK_STATE_FINISHED) {
                succ = false;
                finish = true;
            } else if (wait_until_finished) {
                _wait_for_cancel = true;
                CHECK(
                    wait_on_cancel(),
                    "wait failed, it is only possible when task runs for more than 0x0fffffff ms");

                succ = false;
                finish = true;
            } else {
                succ = false;
                finish = false;
            }
        }
    } else {
        // task cancel itself
        // for timer task, we should set _wait_for_cancel flag to
        // prevent timer task from enqueueing again
        _wait_for_cancel = true;
    }

    if (current_tsk != nullptr) {
        current_tsk->spec().on_task_cancel_post.execute(current_tsk, this, succ);
    }

    if (succ) {
        spec().on_task_cancelled.execute(this);
        signal_waiters();

        // we call clear_callback only cancelling succeed.
        // otherwise, task will successfully exececuted and clear_callback will be called
        // in "exec_internal".
        clear_non_trivial_on_task_end();
    }

    if (finished)
        *finished = finish;

    return succ;
}

const char *task::get_current_node_name()
{
    auto n = get_current_node2();
    return n ? n->full_name() : "unknown";
}

void task::enqueue()
{
    CHECK_NOTNULL(_node, "service node unknown for this task");
    CHECK_NE_MSG(_spec->type,
                 TASK_TYPE_RPC_RESPONSE,
                 "tasks with TASK_TYPE_RPC_RESPONSE type use task::enqueue(caller_pool()) instead");
    CHECK_NE_MSG(_error, ERR_IO_PENDING, "task is waiting for IO, can not be enqueue");

    auto pool = node()->computation()->get_pool(spec().pool_code);
    enqueue(pool);
}

void task::enqueue(task_worker_pool *pool)
{
    this->add_ref(); // released in exec_internal (even when cancelled)

    CHECK_NOTNULL(pool,
                  "pool {} not ready, and there are usually two cases: "
                  "(1). thread pool not designatd in '[{}] pools'; "
                  "(2). the caller is executed in io threads "
                  "which is forbidden unless you explicitly set [task.{}].allow_inline = true",
                  _spec->pool_code,
                  _node->spec().config_section,
                  _spec->name);

    if (spec().type == TASK_TYPE_COMPUTE) {
        spec().on_task_enqueue.execute(get_current_task(), this);
    }

    // for delayed tasks, refering to timer service
    if (_delay_milliseconds != 0) {
        pool->add_timer(this);
        return;
    }

    // fast execution
    if (_is_null) {
        CHECK(_node == task::get_current_node(), "");
        exec_internal();
        return;
    }

    if (_spec->allow_inline) {
        // inlined
        // warning - this may lead to deadlocks, e.g., allow_inlined
        // task tries to get a non-recursive lock that is already hold
        // by the caller task

        if (_node != get_current_node()) {
            tools::node_scoper ns(_node);
            exec_internal();
            return;
        } else {
            exec_internal();
            return;
        }
    }

    // normal path
    pool->enqueue(this);
}

const std::vector<task_worker *> &get_threadpool_threads_info(threadpool_code code)
{
    return dsn::task::get_current_node2()->computation()->get_pool(code)->workers();
}

timer_task::timer_task(
    task_code code, const task_handler &cb, int interval_ms, int hash, service_node *node)
    : task(code, hash, node), _interval_ms(interval_ms), _cb(cb)
{
    DCHECK_GE(_interval_ms, 0);
    CHECK_EQ_MSG(
        TASK_TYPE_COMPUTE,
        spec().type,
        "{} is not a computation type task, please use DEFINE_TASK_CODE to define the task code",
        spec().name);
}

timer_task::timer_task(
    task_code code, task_handler &&cb, int interval_ms, int hash, service_node *node)
    : task(code, hash, node), _interval_ms(interval_ms), _cb(std::move(cb))
{
    DCHECK_GE(_interval_ms, 0);
    CHECK_EQ_MSG(
        TASK_TYPE_COMPUTE,
        spec().type,
        "{} is not a computation type task, please use DEFINE_TASK_CODE to define the task code",
        spec().name);
}

void timer_task::enqueue()
{
    // enable timer randomization to avoid lots of timers execution simultaneously
    if (delay_milliseconds() == 0 && spec().randomize_timer_delay_if_zero) {
        set_delay(rand::next_u32(0, _interval_ms));
    }

    return task::enqueue();
}

void timer_task::exec()
{
    if (dsn_likely(_cb != nullptr)) {
        _cb();
    }
    // valid interval, we reset task state to READY
    if (dsn_likely(_interval_ms > 0)) {
        CHECK(set_retry(true),
              "timer task set retry failed, with state = {}",
              enum_to_string(state()));
        set_delay(_interval_ms);
    }
}

void timer_task::update_interval(int interval_ms)
{
    // Not allowed to set to 0 for periodical task.
    CHECK_GE(interval_ms, 0);
    _interval_ms = interval_ms;
}

} // namespace dsn
