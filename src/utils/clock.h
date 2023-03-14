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

#include <stdint.h>
#include <memory>

namespace dsn {
namespace utils {

class clock
{
public:
    clock() = default;
    virtual ~clock() = default;

    // Gets current time in nanoseconds.
    virtual uint64_t now_ns() const;

    // Gets singleton instance. eager singleton, which is thread safe
    static const clock *instance();

    // Resets the global clock implementation (not thread-safety)
    static void mock(clock *mock_clock);

private:
    static std::unique_ptr<clock> _clock;
};

} // namespace utils
} // namespace dsn
