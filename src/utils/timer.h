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

#include "utils/chrono_literals.h"

namespace dsn {

class timer
{
public:
    timer() = default;

    // Start this timer
    void start()
    {
        _start = std::chrono::system_clock::now();
        _stop = _start;
    }

    // Stop this timer
    void stop() { _stop = std::chrono::system_clock::now(); }

    // Get the elapse from start() to stop(), in various units.
    std::chrono::nanoseconds n_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(_stop - _start);
    }
    std::chrono::microseconds u_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(_stop - _start);
    }
    std::chrono::milliseconds m_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(_stop - _start);
    }
    std::chrono::seconds s_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(_stop - _start);
    }

private:
    std::chrono::time_point<std::chrono::system_clock> _stop;
    std::chrono::time_point<std::chrono::system_clock> _start;
};

} // namespace dsn
