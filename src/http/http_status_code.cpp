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

#include "http/http_status_code.h"

#include <array>
#include <fmt/core.h>

#include "utils/ports.h"

namespace dsn {

const std::array kHttpStatusMessages = {std::string("200 OK"),
                                        std::string("307 Temporary Redirect"),
                                        std::string("400 Bad Request"),
                                        std::string("404 Not Found"),
                                        std::string("500 Internal Server Error")};

static_assert(enum_to_int(http_status_code::kCount) == kHttpStatusMessages.size(),
              "kHttpStatusMessages is not consistent with http_status_code");

std::string get_http_status_message(http_status_code code)
{
    const size_t index = enum_to_int(code);
    if (dsn_unlikely(index >= kHttpStatusMessages.size())) {
        return fmt::format("Unknown http_status_code: index={}", index);
    }

    return kHttpStatusMessages[index];
}

} // namespace dsn
