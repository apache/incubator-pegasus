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

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "builtin_http_calls.h"
#include "http/http_server.h"
#include "http_call_registry.h"
#include "pprof_http_service.h"
#include "service_version.h"
#include "utils/output_utils.h"
#include "utils/process_utils.h"
#include "utils/time_utils.h"

namespace dsn {

/*extern*/ void get_help_handler(const http_request &req, http_response &resp)
{
    utils::table_printer tp;
    std::ostringstream oss;
    auto calls = http_call_registry::instance().list_all_calls();
    for (const auto &call : calls) {
        tp.add_row_name_and_data(std::string("/") + call->path, call->help);
    }
    tp.output(oss, utils::table_printer::output_format::kJsonCompact);
    resp.body = oss.str();
    resp.status_code = http_status_code::ok;
}

/*extern*/ void get_version_handler(const http_request &req, http_response &resp)
{
    std::ostringstream out;
    dsn::utils::table_printer tp;

    tp.add_row_name_and_data("Version", app_version.version);
    tp.add_row_name_and_data("GitCommit", app_version.git_commit);
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);

    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

/*extern*/ void get_recent_start_time_handler(const http_request &req, http_response &resp)
{
    char start_time[100];
    dsn::utils::time_ms_to_date_time(dsn::utils::process_start_millis(), start_time, 100);
    std::ostringstream out;
    dsn::utils::table_printer tp;
    tp.add_row_name_and_data("RecentStartTime", start_time);
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);

    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

/*extern*/ void register_builtin_http_calls()
{
#ifdef DSN_ENABLE_GPERF
    static pprof_http_service pprof_svc;
#endif

    register_http_call("")
        .with_callback(
            [](const http_request &req, http_response &resp) { get_help_handler(req, resp); })
        .with_help("Lists all supported calls");

    register_http_call("version")
        .with_callback(
            [](const http_request &req, http_response &resp) { get_version_handler(req, resp); })
        .with_help("Gets the server version.");

    register_http_call("recentStartTime")
        .with_callback([](const http_request &req, http_response &resp) {
            get_recent_start_time_handler(req, resp);
        })
        .with_help("Gets the server start time.");

    register_http_call("perfCounter")
        .with_callback([](const http_request &req, http_response &resp) {
            get_perf_counter_handler(req, resp);
        })
        .with_help("Gets the value of a perf counter");

    register_http_call("config")
        .with_callback([](const http_request &req, http_response &resp) { get_config(req, resp); })
        .with_help("get the details of a specified config");

    register_http_call("configs")
        .with_callback(
            [](const http_request &req, http_response &resp) { list_all_configs(req, resp); })
        .with_help("list all configs");
}

} // namespace dsn
