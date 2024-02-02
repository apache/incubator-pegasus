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

#include <vector>
#include <cmath>
#include <cstdint>
#include <type_traits>

#include "utils/ports.h"

namespace dsn {
namespace utils {

double mean_stddev(const std::vector<uint32_t> &result_set, bool partial_sample);

template <typename TOutput = int64_t,
          typename TInput = int64_t,
          typename = typename std::enable_if<std::is_arithmetic<TOutput>::value>::type,
          typename = typename std::enable_if<std::is_arithmetic<TInput>::value>::type>
TOutput calc_percentage(TInput numerator, TInput denominator)
{
    if (dsn_unlikely(denominator == 0)) {
        return static_cast<TOutput>(0);
    }

    return static_cast<TOutput>(std::round(numerator * 100.0 / denominator));
}

} // namespace utils
} // namespace dsn
