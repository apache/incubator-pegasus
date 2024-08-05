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

// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <mutex>
#include <unordered_map>
#include <utility>

#include "utils/api_utilities.h"
#include "utils/fail_point.h"
#include "utils/fmt_utils.h"
#include "utils/ports.h"

namespace dsn {
namespace fail {

struct fail_point
{
    enum task_type
    {
        // `action` contain `off()`, which would `close` the fail_point whose `function` passed will
        // not be executed;
        Off,
        // `action` contain `return()`, which would `return` args passed and execute `return` type
        // function passed. it's usually used for `FAIL_POINT_INJECT_F`
        Return,
        // `action` contain `print()`, which would only just print `action` string value and ignore
        // the `function` passed
        Print,
        // `action` contain `void()`, which would return args and execute `function` passed that
        // better mark as `void` type, it's usually used for `FAIL_POINT_INJECT_NOT_RETURN_F` to
        // avoid `return` function
        Void,
    };

    void set_action(std::string_view action);

    const std::string *eval();

    explicit fail_point(std::string_view name) : _name(name) {}

    /// for test only
    fail_point(task_type t, std::string arg, int freq, int max_cnt)
        : _task(t), _arg(std::move(arg)), _freq(freq), _max_cnt(max_cnt)
    {
    }

    /// for test only
    fail_point() = default;

    bool parse_from_string(std::string_view action);

    friend inline bool operator==(const fail_point &p1, const fail_point &p2)
    {
        return p1._task == p2._task && p1._arg == p2._arg && p1._freq == p2._freq &&
               p1._max_cnt == p2._max_cnt;
    }

    task_type get_task() const { return _task; }

    std::string get_arg() const { return _arg; }

    int get_frequency() const { return _freq; }

    int get_max_count() const { return _max_cnt; }

private:
    std::string _name;
    task_type _task{Off};
    std::string _arg;
    int _freq{100};
    int _max_cnt{-1}; // TODO(wutao1): not thread-safe
};
USER_DEFINED_ENUM_FORMATTER(fail_point::task_type)

struct fail_point_registry
{
    fail_point &create_if_not_exists(std::string_view name)
    {
        std::lock_guard<std::mutex> guard(_mu);

        auto it = _registry.emplace(std::string(name), fail_point(name)).first;
        return it->second;
    }

    fail_point *try_get(std::string_view name)
    {
        std::lock_guard<std::mutex> guard(_mu);

        auto it = _registry.find(std::string(name.data(), name.length()));
        if (it == _registry.end()) {
            return nullptr;
        }
        return &it->second;
    }

    void clear()
    {
        std::lock_guard<std::mutex> guard(_mu);
        _registry.clear();
    }

private:
    mutable std::mutex _mu;
    std::unordered_map<std::string, fail_point> _registry;
};

} // namespace fail
} // namespace dsn
