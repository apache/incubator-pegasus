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

#include "http_server.h"
#include "utils/flags.h"
#include "utils/output_utils.h"

namespace dsn {
void update_config(const http_request &req, http_response &resp)
{
    if (req.query_args.size() != 1) {
        resp.status_code = http_status_code::bad_request;
        return;
    }

    auto iter = req.query_args.begin();
    auto res = update_flag(iter->first, iter->second);

    utils::table_printer tp;
    tp.add_row_name_and_data("update_status", res.description());
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void list_all_configs(const http_request &req, http_response &resp)
{
    if (!req.query_args.empty()) {
        resp.status_code = http_status_code::bad_request;
        return;
    }

    resp.body = list_all_flags();
    resp.status_code = http_status_code::ok;
}

void get_config(const http_request &req, http_response &resp)
{
    std::string config_name;
    for (const auto &p : req.query_args) {
        if ("name" == p.first) {
            config_name = p.second;
        } else {
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }

    auto res = get_flag_str(config_name);
    if (res.is_ok()) {
        resp.body = res.get_value();
    } else {
        resp.body = res.get_error().description();
    }
    resp.status_code = http_status_code::ok;
}
} // namespace dsn
