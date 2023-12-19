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

#include <stddef.h>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "utils/enum_helper.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace dsn {

/*
{
    ok,                    // 200
    temporary_redirect,    // 307
    bad_request,           // 400
    not_found,             // 404
    internal_server_error, // 500
    invalid,
};
 */

#define ENUM_FOREACH_HTTP_STATUS_CODE(DEF)                                                         \
    DEF(Ok)                                                                                        \
    DEF(TemporaryRedirect)                                                                         \
    DEF(BadRequest)                                                                                \
    DEF(NotFound)                                                                                  \
    DEF(InternalServerError)

enum class http_status_code : size_t
{
    ENUM_FOREACH_HTTP_STATUS_CODE(ENUM_CONST_DEF) kInvalidCode,
};

#define ENUM_CONST_REG_STR_HTTP_STATUS_CODE(str) ENUM_CONST_REG_STR(http_status_code, str)

ENUM_BEGIN(http_status_code, http_status_code::kInvalidCode)
ENUM_FOREACH_HTTP_STATUS_CODE(ENUM_CONST_REG_STR_HTTP_STATUS_CODE)
ENUM_END(http_status_code)

std::string http_status_code_to_string(http_status_code code);

template <typename TInt, typename = std::enable_if_t<std::is_integral_v<TInt>>>
http_status_code http_status_code_from_int(TInt val)
{
    static const std::unordered_map<TInt, http_status_code> kIntToHttpStatusCodes = {
        {307, http_status_code::kTemporaryRedirect},
        {400, http_status_code::kBadRequest},
        {404, http_status_code::kNotFound},
        {500, http_status_code::kInternalServerError},
    };
    CHECK_EQ(enum_to_int(http_status_code::kInvalidCode), kIntToHttpStatusCodes.size() + 1);

    if (dsn_likely(val == 200)) {
        return http_status_code::kOk;
    }

    const auto &iter = kIntToHttpStatusCodes.find(val);
    if (dsn_unlikely(iter == kIntToHttpStatusCodes.end())) {
        return http_status_code::kInvalidCode;
    }

    return iter->second;
}

} // namespace dsn
