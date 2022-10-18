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

#include "token_buckets.h"

namespace dsn {
namespace utils {

std::shared_ptr<folly::DynamicTokenBucket> token_buckets::get_token_bucket(const std::string &name)
{
    {
        utils::auto_read_lock l(_buckets_lock);
        auto iter = _token_buckets.find(name);
        if (iter != _token_buckets.end()) {
            return iter->second;
        }
    }

    utils::auto_write_lock l(_buckets_lock);
    auto iter = _token_buckets.find(name);
    if (iter != _token_buckets.end()) {
        return iter->second;
    }

    auto token = std::make_shared<folly::DynamicTokenBucket>();
    _token_buckets.emplace(name, token);
    return token;
}
} // namespace utils
} // namespace dsn
