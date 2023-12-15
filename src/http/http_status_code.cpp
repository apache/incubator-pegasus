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

#include <vector>

#include "utils/ports.h"

const std::vector<std::string> kHttpStatusCodeMessages = {"200 OK", "307 Temporary Redirect", "400 Bad Request", "404 Not Found", "500 Internal Server Error"};

std::string http_status_code_to_string(http_status_code code)
{
    if (dsn_likely(static_cast<size_t>(code) < kHttpStatusCodeMessages.size())
    switch (code) {
    case http_status_code::ok:
        return ;
    case http_status_code::temporary_redirect:
        return ;
    case http_status_code::bad_request:
        return ;
    case http_status_code::not_found:
        return ;
    case http_status_code::internal_server_error:
        return ;
    default:
        LOG_FATAL("invalid code: {}", static_cast<int>(code));
        __builtin_unreachable();
    }
}
