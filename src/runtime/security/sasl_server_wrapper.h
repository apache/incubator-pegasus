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

#include "sasl_wrapper.h"

namespace dsn {
namespace security {
class sasl_server_wrapper : public sasl_wrapper
{
public:
    sasl_server_wrapper() = default;
    ~sasl_server_wrapper() = default;

    error_s init();
    error_s start(const std::string &mechanism, const blob &input, blob &output);
    error_s step(const blob &input, blob &output);
};
} // namespace security
} // namespace dsn
