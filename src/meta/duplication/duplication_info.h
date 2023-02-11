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

#include <fmt/core.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common//duplication_common.h"
#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "duplication_types.h"
#include "runtime/rpc/rpc_address.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/zlocks.h"

namespace dsn {
namespace replication {

class app_state;
class duplication_info;

using duplication_info_s_ptr = std::shared_ptr<duplication_info>;

/// This class is thread-safe.
class duplication_info
{
public:
    /// \see meta_duplication_service::new_dup_from_init
    /// \see duplication_info::decode_from_blob
    duplication_info(dupid_t dupid,
                     int32_t appid,
                     std::string app_name,
                     int32_t partition_count,
                     uint64_t create_now_ms,
                     std::string follower_cluster_name,
                     std::vector<rpc_address> &&follower_cluster_metas,
                     std::string meta_store_path)
        : id(dupid),
          app_id(appid),
          app_name(std::move(app_name)),
          partition_count(partition_count),
          follower_cluster_name(std::move(follower_cluster_name)),
          follower_cluster_metas(std::move(follower_cluster_metas)),
          store_path(std::move(meta_store_path)),
          create_timestamp_ms(create_now_ms),
          prefix_for_log(fmt::format("a{}d{}", app_id, id))
    {
        for (int i = 0; i < partition_count; i++) {
            _progress[i] = {};
        }
    }

    error_code start(bool is_duplicating_checkpoint = true)
    {
        if (is_duplicating_checkpoint) {
            return alter_status(duplication_status::DS_PREPARE);
        }
        LOG_WARNING("you now create duplication[{}[{}.{}]] without duplicating checkpoint",
                    id,
                    follower_cluster_name,
                    app_name);
        return alter_status(duplication_status::DS_LOG);
    }

    // error will be returned if this state transition is not allowed.
    error_code
    alter_status(duplication_status::type to_status,
                 duplication_fail_mode::type to_fail_mode = duplication_fail_mode::FAIL_SLOW);

    // call this function after data has been persisted on meta storage.
    void persist_status();

    // not thread-safe
    duplication_status::type status() const { return _status; }
    duplication_fail_mode::type fail_mode() const { return _fail_mode; }

    // if this duplication is in valid status.
    bool is_invalid_status() const { return is_duplication_status_invalid(_status); }

    bool is_valid_alteration(duplication_status::type to_status) const
    {
        return to_status == _status || (to_status == duplication_status::DS_PREPARE &&
                                        _status == duplication_status::DS_INIT) ||
               (to_status == duplication_status::DS_APP &&
                _status == duplication_status::DS_PREPARE) ||
               (to_status == duplication_status::DS_LOG &&
                (_status == duplication_status::DS_PAUSE || _status == duplication_status::DS_APP ||
                 _status == duplication_status::DS_INIT)) ||
               (to_status == duplication_status::DS_PAUSE &&
                _status == duplication_status::DS_LOG) ||
               (to_status == duplication_status::DS_REMOVED);
    };

    ///
    /// alter_progress -> persist_progress
    ///

    // Returns: false if `confirm_entry` is not supposed to be persisted,
    //          maybe because meta storage is busy or `confirm_entry` is stale.
    bool alter_progress(int partition_index, const duplication_confirm_entry &confirm_entry);

    void persist_progress(int partition_index);

    void init_progress(int partition_index, decree confirmed);

    // Generates a json blob to be stored in meta storage.
    // The status in json is `next_status`.
    blob to_json_blob() const;

    /// \see meta_duplication_service::recover_from_meta_state
    static duplication_info_s_ptr decode_from_blob(dupid_t dup_id,
                                                   int32_t app_id,
                                                   const std::string &app_name,
                                                   int32_t partition_count,
                                                   std::string store_path,
                                                   const blob &json);

    // duplication_query_rpc is handled in THREAD_POOL_META_SERVER,
    // which is not thread safe for read.
    void append_if_valid_for_query(const app_state &app,
                                   /*out*/ std::vector<duplication_entry> &entry_list) const;

    duplication_entry to_duplication_entry() const
    {
        duplication_entry entry;
        entry.dupid = id;
        entry.create_ts = create_timestamp_ms;
        entry.remote = follower_cluster_name;
        entry.status = _status;
        entry.__set_fail_mode(_fail_mode);
        entry.__isset.progress = true;
        for (const auto &kv : _progress) {
            if (!kv.second.is_inited) {
                continue;
            }
            entry.progress[kv.first] = kv.second.stored_decree;
        }
        return entry;
    }

    bool all_checkpoint_has_prepared()
    {
        int prepared = 0;
        bool completed =
            std::all_of(_progress.begin(),
                        _progress.end(),
                        [&](std::pair<int, partition_progress> item) -> bool {
                            prepared = item.second.checkpoint_prepared ? prepared + 1 : prepared;
                            return item.second.checkpoint_prepared;
                        });
        if (!completed) {
            LOG_WARNING("replica checkpoint still running: {}/{}", prepared, _progress.size());
        }
        return completed;
    }

    void report_progress_if_time_up();

    // This function should only be used for testing.
    // Not thread-safe.
    bool is_altering() const { return _is_altering; }

    // Test util
    bool equals_to(const duplication_info &rhs) const { return to_string() == rhs.to_string(); }

    // To json encoded string.
    std::string to_string() const;

    const char *log_prefix() const { return prefix_for_log.c_str(); }

private:
    friend class duplication_info_test;
    friend class meta_duplication_service_test;

    // Whether there's ongoing meta storage update.
    bool _is_altering{false};

    mutable zrwlock_nr _lock;

    static constexpr int PROGRESS_UPDATE_PERIOD_MS = 5000;          // 5s
    static constexpr int PROGRESS_REPORT_PERIOD_MS = 1000 * 60 * 5; // 5min

    struct partition_progress
    {
        int64_t volatile_decree{invalid_decree};
        int64_t stored_decree{invalid_decree};
        bool is_altering{false};
        uint64_t last_progress_update_ms{0};
        bool is_inited{false};
        bool checkpoint_prepared{false};
    };

    // partition_idx => progress
    std::map<int, partition_progress> _progress;

    uint64_t _last_progress_report_ms{0};

    duplication_status::type _status{duplication_status::DS_INIT};
    duplication_status::type _next_status{duplication_status::DS_INIT};

    duplication_fail_mode::type _fail_mode{duplication_fail_mode::FAIL_SLOW};
    duplication_fail_mode::type _next_fail_mode{duplication_fail_mode::FAIL_SLOW};
    struct json_helper
    {
        std::string remote;
        duplication_status::type status;
        int64_t create_timestamp_ms;
        duplication_fail_mode::type fail_mode;

        DEFINE_JSON_SERIALIZATION(remote, status, create_timestamp_ms, fail_mode);
    };

public:
    const dupid_t id{0};
    const int32_t app_id{0};
    const std::string app_name;
    const int32_t partition_count{0};

    const std::string follower_cluster_name;
    const std::vector<rpc_address> follower_cluster_metas;
    const std::string store_path; // store path on meta service = get_duplication_path(app, dupid)
    const uint64_t create_timestamp_ms{0}; // the time when this dup is created.
    const std::string prefix_for_log;
};

extern void json_encode(dsn::json::JsonWriter &out, const duplication_status::type &s);

extern bool json_decode(const dsn::json::JsonObject &in, duplication_status::type &s);

extern void json_encode(dsn::json::JsonWriter &out, const duplication_fail_mode::type &s);

extern bool json_decode(const dsn::json::JsonObject &in, duplication_fail_mode::type &s);

} // namespace replication
} // namespace dsn
