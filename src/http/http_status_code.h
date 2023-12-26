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

#include "utils/enum_helper.h"

namespace dsn {

#define ENUM_FOREACH_HTTP_STATUS_CODE(DEF)                                                         \
    DEF(Ok, 200, http_status_code)                                                                 \
    DEF(TemporaryRedirect, 307, http_status_code)                                                  \
    DEF(BadRequest, 400, http_status_code)                                                         \
    DEF(NotFound, 404, http_status_code)                                                           \
    DEF(InternalServerError, 500, http_status_code)

enum class http_status_code : size_t
{
    ENUM_FOREACH_HTTP_STATUS_CODE(ENUM_CONST_DEF) kCount,
    kInvalidCode,
};

#define ENUM_CONST_REG_STR_HTTP_STATUS_CODE(str, ...) ENUM_CONST_REG_STR(http_status_code, str)

ENUM_BEGIN(http_status_code, http_status_code::kInvalidCode)
ENUM_FOREACH_HTTP_STATUS_CODE(ENUM_CONST_REG_STR_HTTP_STATUS_CODE)
ENUM_END(http_status_code)

std::string get_http_status_message(http_status_code code);

ENUM_CONST_DEF_FROM_VAL_FUNC(long, http_status_code, ENUM_FOREACH_HTTP_STATUS_CODE)
ENUM_CONST_DEF_TO_VAL_FUNC(long, http_status_code, ENUM_FOREACH_HTTP_STATUS_CODE)
const long kInvalidHttpStatus = -1;

} // namespace dsn
