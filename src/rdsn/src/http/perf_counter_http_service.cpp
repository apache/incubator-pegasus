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

#include "utils/output_utils.h"
#include "builtin_http_calls.h"

namespace dsn {

void get_perf_counter_handler(const http_request &req, http_response &resp)
{
    std::string perf_counter_name;
    for (const auto &p : req.query_args) {
        if ("name" == p.first) {
            perf_counter_name = p.second;
        } else {
            resp.status_code = http_status_code::bad_request;
            return;
        }
    }

    // get perf counter by perf counter name
    perf_counter_ptr perf_counter = perf_counters::instance().get_counter(perf_counter_name);

    // insert perf counter info into table printer
    dsn::utils::table_printer tp;
    if (perf_counter) {
        tp.add_row_name_and_data("name", perf_counter_name);
        if (COUNTER_TYPE_NUMBER_PERCENTILES == perf_counter->type()) {
            tp.add_row_name_and_data("p99", perf_counter->get_percentile(COUNTER_PERCENTILE_99));
            tp.add_row_name_and_data("p999", perf_counter->get_percentile(COUNTER_PERCENTILE_999));
        } else {
            tp.add_row_name_and_data("value", perf_counter->get_value());
        }
        tp.add_row_name_and_data("type", dsn_counter_type_to_string(perf_counter->type()));
        tp.add_row_name_and_data("description", perf_counter->dsptr());
    }

    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}
} // namespace dsn
