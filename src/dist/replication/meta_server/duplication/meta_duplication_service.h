/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include "dist/replication/meta_server/server_state.h"
#include "dist/replication/meta_server/meta_data.h"

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
class meta_duplication_service
{
public:
    meta_duplication_service(server_state *ss, meta_service *ms) : _state(ss), _meta_svc(ms)
    {
        dassert(_state, "_state should not be null");
        dassert(_meta_svc, "_meta_svc should not be null");
    }

    /// See replication.thrift for possible errors for each rpc.

    void query_duplication_info(const duplication_query_request &, duplication_query_response &);

    void add_duplication(duplication_add_rpc rpc);

    void change_duplication_status(duplication_status_change_rpc rpc);

    void duplication_sync(duplication_sync_rpc rpc);

    // Recover from meta state storage.
    void recover_from_meta_state();

private:
    void do_add_duplication(std::shared_ptr<app_state> &app,
                            duplication_info_s_ptr &dup,
                            duplication_add_rpc &rpc);

    void do_change_duplication_status(std::shared_ptr<app_state> &app,
                                      duplication_info_s_ptr &dup,
                                      duplication_status_change_rpc &rpc);

    void do_restore_duplication(dupid_t dup_id, std::shared_ptr<app_state> app);

    void do_restore_duplication_progress(const duplication_info_s_ptr &dup,
                                         const std::shared_ptr<app_state> &app);

    void get_all_available_app(const node_state &ns,
                               std::map<int32_t, std::shared_ptr<app_state>> &app_map) const;

    void do_update_partition_confirmed(duplication_info_s_ptr &dup,
                                       duplication_sync_rpc &rpc,
                                       int32_t partition_idx,
                                       int64_t confirmed_decree);

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
    std::shared_ptr<duplication_info> new_dup_from_init(const std::string &remote_cluster_name,
                                                        std::shared_ptr<app_state> &app) const;

    // get lock to protect access of app table
    zrwlock_nr &app_lock() const { return _state->_lock; }

    // `duplicating` will be set to true if any dup is valid among app->duplications.
    // ensure app_lock (write lock) is held before calling this function
    static void refresh_duplicating_no_lock(const std::shared_ptr<app_state> &app)
    {
        for (const auto &kv : app->duplications) {
            const auto &dup = kv.second;
            if (dup->is_valid()) {
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
