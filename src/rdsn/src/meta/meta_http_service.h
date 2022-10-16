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

#pragma once

#include <algorithm>

#include "common/json_helper.h"
#include "http/http_server.h"

namespace dsn {
namespace replication {

NON_MEMBER_JSON_SERIALIZATION(
    start_bulk_load_request, app_name, cluster_name, file_provider_type, remote_root_path)

struct manual_compaction_info
{
    std::string app_name;
    std::string type;                        // periodic or once
    int32_t target_level;                    // [-1,num_levels]
    std::string bottommost_level_compaction; // skip or force
    int32_t max_concurrent_running_count;    // 0 means no limit
    std::string trigger_time;                // only used when the type is periodic
    DEFINE_JSON_SERIALIZATION(app_name,
                              type,
                              target_level,
                              bottommost_level_compaction,
                              max_concurrent_running_count,
                              trigger_time)
};

struct usage_scenario_info
{
    std::string app_name;
    std::string scenario; // normal or bulk_load
    DEFINE_JSON_SERIALIZATION(app_name, scenario)
};

class meta_service;
class meta_http_service : public http_service
{
public:
    explicit meta_http_service(meta_service *s) : _service(s)
    {
        register_handler("app",
                         std::bind(&meta_http_service::get_app_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/app?app_name=temp");
        register_handler("app/duplication",
                         std::bind(&meta_http_service::query_duplication_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/app/duplication?name=<app_name>");
        register_handler("apps",
                         std::bind(&meta_http_service::list_app_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/apps");
        register_handler("nodes",
                         std::bind(&meta_http_service::list_node_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/nodes");
        register_handler("cluster",
                         std::bind(&meta_http_service::get_cluster_info_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/cluster");
        register_handler("app_envs",
                         std::bind(&meta_http_service::get_app_envs_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/app_envs?name=temp");
        register_handler("backup_policy",
                         std::bind(&meta_http_service::query_backup_policy_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/backup_policy");
        // request body should be start_bulk_load_request
        register_handler("app/start_bulk_load",
                         std::bind(&meta_http_service::start_bulk_load_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/start_bulk_load");
        register_handler("app/query_bulk_load",
                         std::bind(&meta_http_service::query_bulk_load_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/query_bulk_load?name=temp");
        // request body should be manual_compaction_info
        register_handler("app/start_compaction",
                         std::bind(&meta_http_service::start_compaction_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/start_compaction");
        // request body should be usage_scenario_info
        register_handler("app/usage_scenario",
                         std::bind(&meta_http_service::update_scenario_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/meta/app/usage_scenario");
    }

    std::string path() const override { return "meta"; }

    void get_app_handler(const http_request &req, http_response &resp);
    void list_app_handler(const http_request &req, http_response &resp);
    void list_node_handler(const http_request &req, http_response &resp);
    void get_cluster_info_handler(const http_request &req, http_response &resp);
    void get_app_envs_handler(const http_request &req, http_response &resp);
    void query_backup_policy_handler(const http_request &req, http_response &resp);
    void query_duplication_handler(const http_request &req, http_response &resp);
    void start_bulk_load_handler(const http_request &req, http_response &resp);
    void query_bulk_load_handler(const http_request &req, http_response &resp);
    void start_compaction_handler(const http_request &req, http_response &resp);
    void update_scenario_handler(const http_request &req, http_response &resp);

private:
    // set redirect location if current server is not primary
    bool redirect_if_not_primary(const http_request &req, http_response &resp);

    void update_app_env(const std::string &app_name,
                        const std::vector<std::string> &keys,
                        const std::vector<std::string> &values,
                        http_response &resp);

    meta_service *_service;
};

} // namespace replication
} // namespace dsn
