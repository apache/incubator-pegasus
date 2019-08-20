// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/output_utils.h>

#include "server_info_http_services.h"

namespace dsn {

void version_http_service::get_version_handler(const http_request &req, http_response &resp)
{
    std::ostringstream out;
    dsn::utils::table_printer tp;
    tp.add_row_name_and_data("Version", _version);
    tp.add_row_name_and_data("GitCommit", _git_commit);
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);

    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

void recent_start_time_http_service::get_recent_start_time_handler(const http_request &req,
                                                                   http_response &resp)
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

} // namespace dsn
