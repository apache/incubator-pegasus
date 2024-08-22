/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "utils/time_utils.h"

// TODO(yingchun): refactor this after libfmt upgraded
#include <fmt/chrono.h> // IWYU pragma: keep
// IWYU pragma: no_include <fmt/core.h>
// IWYU pragma: no_include <fmt/format.h>
#include <fmt/printf.h> // IWYU pragma: keep
// IWYU pragma: no_include <algorithm>
// IWYU pragma: no_include <iterator>

namespace dsn {
namespace utils {

/*extern*/ void time_ms_to_string(uint64_t ts_ms, char *str)
{
    struct tm tmp;
    auto ret = get_localtime(ts_ms, &tmp);
    // NOTE: format_to() does not append a terminating null character, so remember to initialize
    // str's memory as zero before.
    fmt::format_to(str, "{:%Y-%m-%d %H:%M:%S}.{:03}", *ret, static_cast<uint32_t>(ts_ms % 1000));
}

/*extern*/ void time_ms_to_string(uint64_t ts_ms, std::string &str)
{
    str.clear();
    struct tm tmp;
    auto ret = get_localtime(ts_ms, &tmp);
    fmt::format_to(std::back_inserter(str),
                   "{:%Y-%m-%d %H:%M:%S}.{:03}",
                   *ret,
                   static_cast<uint32_t>(ts_ms % 1000));
}

/*extern*/ void time_ms_to_sequent_string(uint64_t ts_ms, std::string &str)
{
    str.clear();
    struct tm tmp;
    auto ret = get_localtime(ts_ms, &tmp);
    fmt::format_to(std::back_inserter(str),
                   "{:%Y%m%d_%H%M%S}_{:03}",
                   *ret,
                   static_cast<uint32_t>(ts_ms % 1000));
}

} // namespace utils
} // namespace dsn
