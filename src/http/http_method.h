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

#include "utils/enum_helper.h"

namespace dsn {

enum class http_method
{
    GET = 1,
    POST = 2,
    INVALID = 100,
};

enum class http_auth_type
{
    NONE,
    BASIC,
    DIGEST,
    SPNEGO,
};

ENUM_BEGIN(http_method, http_method::INVALID)
ENUM_REG2(http_method, GET)
ENUM_REG2(http_method, POST)
ENUM_END(http_method)

} // namespace dsn
