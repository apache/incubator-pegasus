// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <nlohmann/json.hpp>
#include <fmt/format.h>
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
        };
    }
    resp.status_code = http_status_code::ok;
    resp.body = json.dump();
}

} // namespace replication
} // namespace dsn
