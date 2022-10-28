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

#include "utils/fmt_logging.h"
#include "http_server.h"
#include "utils/errors.h"

namespace dsn {

// A singleton registry for all the HTTP calls
class http_call_registry : public utils::singleton<http_call_registry>
{
public:
    std::shared_ptr<http_call> find(const std::string &path) const
    {
        std::lock_guard<std::mutex> guard(_mu);
        auto it = _call_map.find(path);
        if (it == _call_map.end()) {
            return nullptr;
        }
        return it->second;
    }

    void remove(const std::string &path)
    {
        std::lock_guard<std::mutex> guard(_mu);
        _call_map.erase(path);
    }

    void add(std::unique_ptr<http_call> call_uptr)
    {
        auto call = std::shared_ptr<http_call>(call_uptr.release());
        std::lock_guard<std::mutex> guard(_mu);
        CHECK_EQ(_call_map.count(call->path), 0);
        _call_map[call->path] = call;
    }

    std::vector<std::shared_ptr<http_call>> list_all_calls() const
    {
        std::lock_guard<std::mutex> guard(_mu);

        std::vector<std::shared_ptr<http_call>> ret;
        for (const auto &kv : _call_map) {
            ret.push_back(kv.second);
        }
        return ret;
    }

private:
    friend class utils::singleton<http_call_registry>;
    http_call_registry() = default;
    ~http_call_registry() = default;

private:
    mutable std::mutex _mu;
    std::map<std::string, std::shared_ptr<http_call>> _call_map;
};

} // namespace dsn
