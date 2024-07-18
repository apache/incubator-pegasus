// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>

#include "utils/ports.h"

namespace dsn {

template <typename T>
class simple_concurrent_queue
{
public:
    using value_type = T;

    simple_concurrent_queue() = default;

    ~simple_concurrent_queue() = default;

    void push(value_type &&value)
    {
        {
            std::lock_guard<std::mutex> lock(_mtx);
            _queue.push(std::forward<value_type>(value));
        }

        _cond_var.notify_one();
    }

    void pop(value_type &value)
    {
        std::unique_lock<std::mutex> lock(_mtx);
        _cond_var.wait(lock, [this] { return !_queue.empty(); });

        value = _queue.front();
        _queue.pop();
    }

    bool pop(uint64_t timeout_ns, value_type &value)
    {
        std::unique_lock<std::mutex> lock(_mtx);
        const auto status = _cond_var.wait_for(
            lock, std::chrono::nanoseconds(timeout_ns), [this] { return !_queue.empty(); });

        if (status == std::cv_status::timeout) {
            return false;
        }

        value = _queue.front();
        _queue.pop();
        return true;
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> lock(_mtx);
        return _queue.size();
    }

private:
    std::queue<value_type> _queue;
    mutable std::mutex _mtx;
    mutable std::condition_variable _cond_var;

    DISALLOW_COPY_AND_ASSIGN(simple_concurrent_queue);
};

} // namespace dsn
