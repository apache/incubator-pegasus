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

#include "meta/server_state.h"
#include "meta/meta_data.h"

namespace dsn {
namespace replication {

/// On meta storage, duplication info are stored in the following layout:
///
///   <app_path>/duplication/<dup_id> -> {
///                                         "remote": ...,
///                                         "status": ...,
///                                         "create_timestamp_ms": ...,
///                                      }
///
///   <app_path>/duplication/<dup_id>/<partition_index> -> <confirmed_decree>
///
/// Each app has an attribute called "duplicating" which indicates
/// whether this app should prevent its unconfirmed WAL from being compacted.
///

/// Ref-Issue: https://github.com/apache/incubator-pegasus/issues/892
class meta_duplication_service
{
public:
    meta_duplication_service(server_state *ss, meta_service *ms) : _state(ss), _meta_svc(ms)
    {
        CHECK_NOTNULL(_state, "_state should not be null");
        CHECK_NOTNULL(_meta_svc, "_meta_svc should not be null");
    }

    /// See replication.thrift for possible errors for each rpc.

    void query_duplication_info(const duplication_query_request &, duplication_query_response &);

    void add_duplication(duplication_add_rpc rpc);

    void modify_duplication(duplication_modify_rpc rpc);

    void duplication_sync(duplication_sync_rpc rpc);

    // Recover from meta state storage.
    void recover_from_meta_state();

private:
    void do_add_duplication(std::shared_ptr<app_state> &app,
                            duplication_info_s_ptr &dup,
                            duplication_add_rpc &rpc);

    void do_modify_duplication(std::shared_ptr<app_state> &app,
                               duplication_info_s_ptr &dup,
                               duplication_modify_rpc &rpc);

    void do_restore_duplication(dupid_t dup_id, std::shared_ptr<app_state> app);

    void do_restore_duplication_progress(const duplication_info_s_ptr &dup,
                                         const std::shared_ptr<app_state> &app);

    void get_all_available_app(const node_state &ns,
                               std::map<int32_t, std::shared_ptr<app_state>> &app_map) const;

    void do_update_partition_confirmed(duplication_info_s_ptr &dup,
                                       duplication_sync_rpc &rpc,
                                       int32_t partition_idx,
                                       const duplication_confirm_entry &confirm_entry);

    void create_follower_app_for_duplication(const std::shared_ptr<duplication_info> &dup,
                                             const std::shared_ptr<app_state> &app);
    void check_follower_app_if_create_completed(const std::shared_ptr<duplication_info> &dup);

    // Get zk path for duplication.
    std::string get_duplication_path(const app_state &app) const
    {
        return _state->get_app_path(app) + "/duplication";
    }
    std::string get_duplication_path(const app_state &app, const std::string &dupid) const
    {
        return get_duplication_path(app) + "/" + dupid;
    }
    static std::string get_partition_path(const duplication_info_s_ptr &dup,
                                          const std::string &partition_idx)
    {
        return dup->store_path + "/" + partition_idx;
    }

    // Create a new duplication from INIT state.
    // Thread-Safe
    std::shared_ptr<duplication_info>
    new_dup_from_init(const std::string &follower_cluster_name,
                      std::vector<rpc_address> &&follower_cluster_metas,
                      std::shared_ptr<app_state> &app) const;

    // get lock to protect access of app table
    zrwlock_nr &app_lock() const { return _state->_lock; }

    // `duplicating` will be set to true if any dup is valid among app->duplications.
    // ensure app_lock (write lock) is held before calling this function
    static void refresh_duplicating_no_lock(const std::shared_ptr<app_state> &app)
    {
        for (const auto &kv : app->duplications) {
            const auto &dup = kv.second;
            if (!dup->is_invalid_status()) {
                app->__set_duplicating(true);
                return;
            }
        }
        app->__set_duplicating(false);
    }

private:
    friend class meta_duplication_service_test;

    server_state *_state;

    meta_service *_meta_svc;
};

} // namespace replication
} // namespace dsn
