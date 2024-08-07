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

#include "sasl_server_wrapper.h"

#include <sasl/sasl.h>

#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/flags.h"
#include <string_view>

DSN_DECLARE_string(service_fqdn);
DSN_DECLARE_string(service_name);

namespace dsn {
namespace security {

error_s sasl_server_wrapper::init()
{
    FAIL_POINT_INJECT_F("sasl_server_wrapper_init", [](std::string_view str) {
        error_code err = error_code::try_get(str.data(), ERR_UNKNOWN);
        return error_s::make(err);
    });

    int sasl_err = sasl_server_new(
        FLAGS_service_name, FLAGS_service_fqdn, nullptr, nullptr, nullptr, nullptr, 0, &_conn);
    return wrap_error(sasl_err);
}

error_s sasl_server_wrapper::start(const std::string &mechanism, const blob &input, blob &output)
{
    FAIL_POINT_INJECT_F("sasl_server_wrapper_start", [](std::string_view str) {
        error_code err = error_code::try_get(str.data(), ERR_UNKNOWN);
        return error_s::make(err);
    });

    const char *msg = nullptr;
    unsigned msg_len = 0;
    int sasl_err =
        sasl_server_start(_conn, mechanism.c_str(), input.data(), input.length(), &msg, &msg_len);

    output = blob::create_from_bytes(msg, msg_len);
    return wrap_error(sasl_err);
}

error_s sasl_server_wrapper::step(const blob &input, blob &output)
{
    FAIL_POINT_INJECT_F("sasl_server_wrapper_step", [](std::string_view str) {
        error_code err = error_code::try_get(str.data(), ERR_UNKNOWN);
        return error_s::make(err);
    });

    const char *msg = nullptr;
    unsigned msg_len = 0;
    int sasl_err = sasl_server_step(_conn, input.data(), input.length(), &msg, &msg_len);

    output = blob::create_from_bytes(msg, msg_len);
    return wrap_error(sasl_err);
}
} // namespace security
} // namespace dsn
