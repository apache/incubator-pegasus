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

#include "clock.h"

#include "runtime/api_layer1.h"
#include "time_utils.h"

uint64_t dsn_now_ns() { return dsn::utils::clock::instance()->now_ns(); }

namespace dsn {
namespace utils {

std::unique_ptr<clock> clock::_clock = std::make_unique<clock>();

const clock *clock::instance() { return _clock.get(); }

uint64_t clock::now_ns() const { return get_current_physical_time_ns(); }

void clock::mock(clock *mock_clock) { _clock.reset(mock_clock); }

} // namespace utils
} // namespace dsn
