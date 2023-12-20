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

#include <memory>
#include <string>

#include "utils/errors.h"

namespace dsn {
class blob;
} // namespace dsn

typedef struct sasl_conn sasl_conn_t;

namespace dsn {
namespace security {
class sasl_wrapper
{
public:
    virtual ~sasl_wrapper();

    virtual error_s init() = 0;
    virtual error_s start(const std::string &mechanism, const blob &input, blob &output) = 0;
    virtual error_s step(const blob &input, blob &output) = 0;
    /**
     * retrieve username from sasl connection.
     * If this is a sasl server, it gets the name of the corresponding sasl client.
     * But if this is a sasl client, it gets the name of itself
     **/
    error_s retrieve_username(/*out*/ std::string &output);

protected:
    sasl_wrapper() = default;

    // wrap_error wraps a sasl error with full description.
    error_s wrap_error(int sasl_err);

    sasl_conn_t *_conn = nullptr;
};

std::unique_ptr<sasl_wrapper> create_sasl_wrapper(bool is_client);
} // namespace security
} // namespace dsn
