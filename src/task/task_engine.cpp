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

#include "task_engine.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <limits.h>
#include <mutex>

#include "fmt/core.h"
#include "nlohmann/json.hpp"
#include "runtime/global_config.h"
#include "runtime/service_engine.h"
#include "task.h"
#include "task/task_code.h"
#include "task_queue.h"
#include "task_spec.h"
#include "task_worker.h"
#include "timer_service.h"
#include "utils/command_manager.h"
#include "utils/factory_store.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/string_conv.h"
#include "utils/threadpool_code.h"

using dsn::utils::factory_store;

namespace dsn {

task_worker_pool::task_worker_pool(const threadpool_spec &opts, task_engine *owner)
    : _spec(opts), _owner(owner), _node(owner->node()), _is_running(false)
{
}

void task_worker_pool::create()
{
    if (_is_running)
        return;

    int qCount = _spec.partitioned ? _spec.worker_count : 1;
    for (int i = 0; i < qCount; i++) {
        auto q = factory_store<task_queue>::create(
            _spec.queue_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, i, nullptr);
        for (auto it = _spec.queue_aspects.begin(); it != _spec.queue_aspects.end(); ++it) {
            q = factory_store<task_queue>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, i, q);
        }
        _queues.push_back(q);
    }

    for (int i = 0; i < qCount; ++i) {
        auto tsvc = factory_store<timer_service>::create(
            service_engine::instance().spec().timer_factory_name.c_str(),
            PROVIDER_TYPE_MAIN,
            _node,
            nullptr);
        _per_queue_timer_svcs.push_back(tsvc);
    }

    for (int i = 0; i < _spec.worker_count; i++) {
        auto q = _queues[qCount == 1 ? 0 : i];
        task_worker *worker = factory_store<task_worker>::create(
            _spec.worker_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, q, i, nullptr);
        for (auto it = _spec.worker_aspects.begin(); it != _spec.worker_aspects.end(); ++it) {
            worker = factory_store<task_worker>::create(
                it->c_str(), PROVIDER_TYPE_ASPECT, this, q, i, worker);
        }
        task_worker::on_create.execute(worker);

        _workers.push_back(worker);
    }
}

void task_worker_pool::start()
{
    if (_is_running)
        return;

    for (auto &tsvc : _per_queue_timer_svcs) {
        tsvc->start();
    }
    for (auto &wk : _workers) {
        wk->start();
    }

    LOG_INFO(
        "[{}]: thread pool [{}] started, pool_code = {}, worker_count = {}, worker_share_core = "
        "{}, partitioned = {}, ...",
        _node->full_name(),
        _spec.name,
        _spec.pool_code,
        _spec.worker_count,
        _spec.worker_share_core ? "true" : "false",
        _spec.partitioned ? "true" : "false");

    _is_running = true;
}

void task_worker_pool::stop()
{
    if (!_is_running) {
        return;
    }

    for (auto &tsvc : _per_queue_timer_svcs) {
        tsvc->stop();
    }
    for (auto &wk : _workers) {
        wk->stop();
    }
    _is_running = false;
    LOG_INFO("[{}]: thread pool {} stopped", _node->full_name(), _spec.name);
}

void task_worker_pool::add_timer(task *t)
{
    CHECK_GT_MSG(
        t->delay_milliseconds(), 0, "task delayed should be dispatched to timer service first");

    unsigned int idx =
        (_spec.partitioned ? static_cast<unsigned int>(t->hash()) %
                                 static_cast<unsigned int>(_per_queue_timer_svcs.size())
                           : 0);
    _per_queue_timer_svcs[idx]->add_timer(t);
}

void task_worker_pool::enqueue(task *t)
{
    CHECK(t->spec().pool_code == spec().pool_code || t->spec().type == TASK_TYPE_RPC_RESPONSE,
          "Invalid thread pool used");
    CHECK_EQ_MSG(
        t->delay_milliseconds(), 0, "task delayed should be dispatched to timer service first");

    CHECK(_is_running,
          "worker pool {} must be started before enqueue task {}",
          spec().name,
          t->spec().name);
    unsigned int idx = (_spec.partitioned ? static_cast<unsigned int>(t->hash()) %
                                                static_cast<unsigned int>(_queues.size())
                                          : 0);
    return _queues[idx]->enqueue_internal(t);
}

bool task_worker_pool::shared_same_worker_with_current_task(task *tsk) const
{
    task *current = task::get_current_task();
    if (nullptr != current) {
        if (current->spec().pool_code != tsk->code())
            return false;
        else if (_workers.size() == 1)
            return true;
        else if (_spec.partitioned) {
            unsigned int sz = static_cast<unsigned int>(_workers.size());
            return static_cast<unsigned int>(current->hash()) % sz ==
                   static_cast<unsigned int>(tsk->hash()) % sz;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

nlohmann::json task_worker_pool::get_runtime_info(const std::vector<std::string> &args) const
{
    nlohmann::json info;

    // Queues.
    nlohmann::json queues;
    for (const auto &queue : _queues) {
        if (queue) {
            nlohmann::json q;
            q["name"] = queue->get_name();
            q["pending_task_count"] = queue->count();
            queues.emplace_back(q);
        }
    }
    info["queues"] = queues;

    // Threads.
    nlohmann::json workers;
    for (const auto &worker : _workers) {
        if (worker) {
            nlohmann::json w;
            w["index"] = worker->index();
            w["TID"] = worker->native_tid();
            w["queue_name"] = worker->queue()->get_name();
            workers.emplace_back(w);
        }
    }
    info["threads"] = workers;

    return info;
}

nlohmann::json task_worker_pool::get_queue_info() const
{
    nlohmann::json queues;
    for (const auto &queue : _queues) {
        if (queue) {
            nlohmann::json q;
            q["name"] = queue->get_name();
            q["pending_task_count"] = queue->count();
            queues.emplace_back(q);
        }
    }
    return queues;
}

task_engine::task_engine(service_node *node)
{
    _is_running = false;
    _node = node;
    register_cli_commands();
}

void task_engine::create(const std::list<threadpool_code> &pools)
{
    if (_is_running)
        return;

    // init pools
    _pools.resize(threadpool_code::max() + 1, nullptr);
    for (auto &p : pools) {
        auto &s = service_engine::instance().spec().threadpool_specs[p];
        auto workerPool = new task_worker_pool(s, this);
        workerPool->create();
        // TODO(Tang yanzhao): _pools[_pools.size()-1] is always be null, fix it.
        _pools[p] = workerPool;
    }
}

void task_engine::start()
{
    if (_is_running)
        return;

    for (auto &pl : _pools) {
        if (pl)
            pl->start();
    }
    _is_running = true;
    LOG_INFO("[{}]: task engine started", _node->full_name());
}

void task_engine::stop()
{
    if (!_is_running) {
        return;
    }

    for (auto &pl : _pools) {
        if (pl)
            pl->stop();
    }
    _is_running = false;
    LOG_INFO("[{}]: task engine stopped", _node->full_name());
}

volatile int *task_engine::get_task_queue_virtual_length_ptr(dsn::task_code code, int hash)
{
    auto pl = get_pool(task_spec::get(code)->pool_code);
    auto idx = (pl->spec().partitioned ? hash % pl->spec().worker_count : 0);
    return pl->queues()[idx]->get_virtual_length_ptr();
}

nlohmann::json task_engine::get_runtime_info(const std::vector<std::string> &args) const
{
    nlohmann::json pools;
    for (const auto &pool : _pools) {
        if (pool) {
            pools[pool->spec().pool_code.to_string()] = pool->get_runtime_info(args);
        }
    }
    return pools;
}

nlohmann::json task_engine::get_queue_info() const
{
    nlohmann::json pools;
    for (const auto &pool : _pools) {
        if (pool) {
            nlohmann::json p;
            p["name"] = pool->spec().pool_code.to_string();
            p["queue"] = pool->get_queue_info();
            pools.emplace_back(p);
        }
    }
    return pools;
}

void task_engine::register_cli_commands()
{
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _task_queue_max_length_cmd = dsn::command_manager::instance().register_single_command(
            "task.queue_max_length",
            "Get the current or set a new max task queue length of a specific thread_pool. It can "
            "be set it to INT_MAX which means a big enough value, but it can't be cancelled the "
            "delay/reject policy dynamically",
            "<pool_code> [new_max_queue_length]",
            [this](const std::vector<std::string> &args) {
                if (args.empty()) {
                    return std::string("ERR: invalid arguments, task.queue_max_length <pool_code> "
                                       "[queue_max_length]");
                }

                for (const auto &pool : _pools) {
                    if (!pool) {
                        continue;
                    }
                    if (pool->_spec.pool_code.to_string() == args[0]) {
                        // Query.
                        if (args.size() == 1) {
                            return fmt::format("The current task queue length of {} is {}",
                                               args[0],
                                               pool->_spec.queue_length_throttling_threshold);
                        }

                        // Update.
                        if (args.size() == 2) {
                            int new_queue_length = INT_MAX;
                            if ((args[1] != "INT_MAX" &&
                                 !dsn::buf2int32(args[1], new_queue_length)) ||
                                new_queue_length < 0) {
                                return fmt::format("queue_max_length must be >= 0 or 'INT_MAX'");
                            }
                            pool->_spec.queue_length_throttling_threshold = new_queue_length;
                            return fmt::format("Task queue {} is updated to new max length {}",
                                               args[0],
                                               pool->_spec.queue_length_throttling_threshold);
                        }
                    }
                }
                return std::string("ERR: thread_pool not found");
            });
    });
}

} // namespace dsn
