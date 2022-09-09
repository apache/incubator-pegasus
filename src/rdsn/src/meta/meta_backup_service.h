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

#include "meta_backup_engine.h"
#include "meta_service.h"
#include "meta_state_service_utils.h"
#include "server_state.h"

namespace dsn {
namespace replication {

// TODO(heyuchen): implement it
class backup_service
{
public:
    explicit backup_service(meta_service *meta_svc, const std::string &remote_storage_root);
    virtual ~backup_service();

    void start();

    void start_backup_app(start_backup_app_rpc rpc);
    void query_backup_status(query_backup_status_rpc rpc);

    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_state() const { return _state; }

private:
    void create_onetime_backup_on_remote_storage(std::shared_ptr<meta_backup_engine> engine,
                                                 start_backup_app_rpc rpc);

    void get_node_path(const int32_t app_id,
                       const bool is_periodic,
                       /*out*/ std::queue<std::string> &nodes,
                       const int64_t backup_id = 0) const
    {
        nodes.push(_remote_storage_root);
        nodes.push(std::to_string(app_id));
        nodes.push(is_periodic ? PERIODIC_PATH : ONETIME_PATH);
        if (backup_id > 0) {
            nodes.push(std::to_string(backup_id));
        }
    }

private:
    friend class backup_engine;
    friend class meta_backup_service_test;

    meta_service *_meta_svc;
    server_state *_state;

    std::string _remote_storage_root;
    task_tracker _tracker;

    zrwlock_nr _lock; // {
    std::vector<std::shared_ptr<meta_backup_engine>> _onetime_backup_states;
    // }
};
} // namespace replication
} // namespace dsn
