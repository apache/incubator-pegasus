// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <nlohmann/json.hpp>
#include <fmt/format.h>
#include <dsn/utility/output_utils.h>
#include "replica_http_service.h"
#include "duplication/duplication_sync_timer.h"

namespace dsn {
namespace replication {

void replica_http_service::query_duplication_handler(const http_request &req, http_response &resp)
{
    if (!_stub->_duplication_sync_timer) {
        resp.body = "duplication is not enabled [duplication_enabled=false]";
        resp.status_code = http_status_code::not_found;
        return;
    }
    auto it = req.query_args.find("appid");
    if (it == req.query_args.end()) {
        resp.body = "appid should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }
    int32_t appid = -1;
    if (!buf2int32(it->second, appid) || appid < 0) {
        resp.status_code = http_status_code::bad_request;
        resp.body = fmt::format("invalid appid={}", it->second);
        return;
    }
    bool app_found = false;
    auto states = _stub->_duplication_sync_timer->get_dup_states(appid, &app_found);
    if (!app_found) {
        resp.status_code = http_status_code::not_found;
        resp.body = fmt::format("no primary for app [appid={}]", appid);
        return;
    }
    if (states.empty()) {
        resp.status_code = http_status_code::not_found;
        resp.body = fmt::format("no duplication assigned for app [appid={}]", appid);
        return;
    }

    nlohmann::json json;
    for (const auto &s : states) {
        json[std::to_string(s.first)][s.second.id.to_string()] = nlohmann::json{
            {"duplicating", s.second.duplicating},
            {"not_confirmed_mutations_num", s.second.not_confirmed},
            {"not_duplicated_mutations_num", s.second.not_duplicated},
            {"fail_mode", duplication_fail_mode_to_string(s.second.fail_mode)},
        };
    }
    resp.status_code = http_status_code::ok;
    resp.body = json.dump();
}

void replica_http_service::query_app_data_version_handler(const http_request &req,
                                                          http_response &resp)
{
    auto it = req.query_args.find("app_id");
    if (it == req.query_args.end()) {
        resp.body = "app_id should not be empty";
        resp.status_code = http_status_code::bad_request;
        return;
    }

    int32_t app_id = -1;
    if (!buf2int32(it->second, app_id) || app_id < 0) {
        resp.body = fmt::format("invalid app_id={}", it->second);
        resp.status_code = http_status_code::bad_request;
        return;
    }

    uint32_t data_version = 0;
    error_code ec = _stub->query_app_data_version(app_id, data_version);

    dsn::utils::table_printer tp;
    tp.add_row_name_and_data("error", ec.to_string());
    tp.add_row_name_and_data("data_version", data_version);
    std::ostringstream out;
    tp.output(out, dsn::utils::table_printer::output_format::kJsonCompact);
    resp.body = out.str();
    resp.status_code = http_status_code::ok;
}

} // namespace replication
} // namespace dsn
