// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/output_utils.h>
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
