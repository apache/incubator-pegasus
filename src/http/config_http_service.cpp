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

#include <string>
#include <unordered_map>
#include <utility>

#include "http/http_status_code.h"
#include "http_server.h"
#include "utils/errors.h"
#include "utils/flags.h"

namespace dsn {
void list_all_configs(const http_request &req, http_response &resp)
{
    if (!req.query_args.empty()) {
        resp.status_code = http_status_code::kBadRequest;
        return;
    }

    resp.body = list_all_flags();
    resp.status_code = http_status_code::kOk;
}

void get_config(const http_request &req, http_response &resp)
{
    std::string config_name;
    for (const auto &p : req.query_args) {
        if ("name" == p.first) {
            config_name = p.second;
        } else {
            resp.status_code = http_status_code::kBadRequest;
            return;
        }
    }

    if (config_name.empty()) {
        resp.status_code = http_status_code::kBadRequest;
        resp.body = "name shouldn't be empty";
        return;
    }

    auto res = get_flag_str(config_name);
    if (res.is_ok()) {
        resp.body = res.get_value();
    } else {
        resp.body = res.get_error().description();
    }
    resp.status_code = http_status_code::kOk;
}
} // namespace dsn
