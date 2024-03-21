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

#include "replica/replica_http_service.h"

#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <stdint.h>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "duplication/duplication_sync_timer.h"
#include "http/http_server.h"
#include "http/http_status_code.h"
#include "replica/replica_stub.h"
#include "utils/string_conv.h"

namespace dsn {
namespace replication {

void replica_http_service::query_duplication_handler(const http_request &req, http_response &resp)
{
    if (!_stub->_duplication_sync_timer) {
        resp.body = "duplication is not enabled [FLAGS_duplication_enabled=false]";
        resp.status_code = http_status_code::kNotFound;
        return;
    }
    auto it = req.query_args.find("appid");
    if (it == req.query_args.end()) {
        resp.body = "appid should not be empty";
        resp.status_code = http_status_code::kBadRequest;
        return;
    }
    int32_t appid = -1;
    if (!buf2int32(it->second, appid) || appid < 0) {
        resp.status_code = http_status_code::kBadRequest;
        resp.body = fmt::format("invalid appid={}", it->second);
        return;
    }
    bool app_found = false;
    auto states = _stub->_duplication_sync_timer->get_dup_states(appid, &app_found);
    if (!app_found) {
        resp.status_code = http_status_code::kNotFound;
        resp.body = fmt::format("no primary for app [appid={}]", appid);
        return;
    }
    if (states.empty()) {
        resp.status_code = http_status_code::kNotFound;
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
            {"remote_app_name", s.second.remote_app_name},
        };
    }
    resp.status_code = http_status_code::kOk;
    resp.body = json.dump();
}

void replica_http_service::query_app_data_version_handler(const http_request &req,
                                                          http_response &resp)
{
    auto it = req.query_args.find("app_id");
    if (it == req.query_args.end()) {
        resp.body = "app_id should not be empty";
        resp.status_code = http_status_code::kBadRequest;
        return;
    }

    int32_t app_id = -1;
    if (!buf2int32(it->second, app_id) || app_id < 0) {
        resp.body = fmt::format("invalid app_id={}", it->second);
        resp.status_code = http_status_code::kBadRequest;
        return;
    }

    // partition_index -> data_version
    std::unordered_map<int32_t, uint32_t> version_map;
    _stub->query_app_data_version(app_id, version_map);

    if (version_map.size() == 0) {
        resp.body = fmt::format("app_id={} not found", it->second);
        resp.status_code = http_status_code::kNotFound;
        return;
    }

    nlohmann::json json;
    for (const auto &kv : version_map) {
        json[std::to_string(kv.first)] = nlohmann::json{
            {"data_version", std::to_string(kv.second)},
        };
    }
    resp.status_code = http_status_code::kOk;
    resp.body = json.dump();
}

void replica_http_service::query_manual_compaction_handler(const http_request &req,
                                                           http_response &resp)
{
    auto it = req.query_args.find("app_id");
    if (it == req.query_args.end()) {
        resp.body = "app_id should not be empty";
        resp.status_code = http_status_code::kBadRequest;
        return;
    }

    int32_t app_id = -1;
    if (!buf2int32(it->second, app_id) || app_id < 0) {
        resp.body = fmt::format("invalid app_id={}", it->second);
        resp.status_code = http_status_code::kBadRequest;
        return;
    }

    std::unordered_map<gpid, manual_compaction_status::type> partition_compaction_status;
    _stub->query_app_manual_compact_status(app_id, partition_compaction_status);

    int32_t idle_count = 0;
    int32_t running_count = 0;
    int32_t queuing_count = 0;
    int32_t finished_count = 0;
    for (const auto &kv : partition_compaction_status) {
        if (kv.second == manual_compaction_status::RUNNING) {
            running_count++;
        } else if (kv.second == manual_compaction_status::QUEUING) {
            queuing_count++;
        } else if (kv.second == manual_compaction_status::FINISHED) {
            finished_count++;
        } else if (kv.second == manual_compaction_status::IDLE) {
            idle_count++;
        }
    }

    nlohmann::json json;
    json["status"] = nlohmann::json{
        {manual_compaction_status_to_string(manual_compaction_status::IDLE), idle_count},
        {manual_compaction_status_to_string(manual_compaction_status::RUNNING), running_count},
        {manual_compaction_status_to_string(manual_compaction_status::QUEUING), queuing_count},
        {manual_compaction_status_to_string(manual_compaction_status::FINISHED), finished_count}};
    resp.status_code = http_status_code::kOk;
    resp.body = json.dump();
}

void replica_http_service::update_config(const std::string &name) { _stub->update_config(name); }

} // namespace replication
} // namespace dsn
