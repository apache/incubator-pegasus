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

#include <dsn/dist/replication/replication_types.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/cpp/json_helper.h>
#include <dsn/tool-api/zlocks.h>

#include <utility>
#include <fmt/format.h>

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
                     int32_t partition_count,
                     uint64_t create_now_ms,
                     std::string remote_cluster_name,
                     std::string meta_store_path)
        : id(dupid),
          app_id(appid),
          remote(std::move(remote_cluster_name)),
          store_path(std::move(meta_store_path)),
          create_timestamp_ms(create_now_ms)
    {
        for (int i = 0; i < partition_count; i++) {
            _progress[i] = {};
        }
    }

    duplication_info() = default;

    void start()
    {
        zauto_write_lock l(_lock);
        _is_altering = true;
        _next_status = duplication_status::DS_START;
    }

    // change current status to `to_status`.
    // error will be returned if this state transition is not allowed.
    error_code alter_status(duplication_status::type to_status)
    {
        zauto_write_lock l(_lock);
        return do_alter_status(to_status);
    }

    // persist current status to `next_status`
    // call this function after data has been persisted on meta storage.
    void persist_status();

    // if this duplication is in valid status.
    bool is_valid() const { return is_duplication_status_valid(_status); }

    ///
    /// alter_progress -> persist_progress
    ///

    // Returns: false if `d` is not supposed to be persisted,
    //          maybe because meta storage is busy or `d` is stale.
    bool alter_progress(int partition_index, decree d);

    void persist_progress(int partition_index);

    void init_progress(int partition_index, decree confirmed);

    // Generates a json blob to be stored in meta storage.
    // The status in json is `next_status`.
    blob to_json_blob() const;

    /// \see meta_duplication_service::recover_from_meta_state
    static duplication_info_s_ptr decode_from_blob(dupid_t dup_id,
                                                   int32_t app_id,
                                                   int32_t partition_count,
                                                   std::string store_path,
                                                   const blob &json);

    // duplication_query_rpc is handled in THREAD_POOL_META_SERVER,
    // which is not thread safe for read.
    void append_if_valid_for_query(
        /*out*/ std::vector<duplication_entry> &entry_list) const
    {
        zauto_read_lock l(_lock);
        entry_list.emplace_back(to_duplication_entry());
    }

    duplication_entry to_duplication_entry() const
    {
        duplication_entry entry;
        entry.dupid = id;
        entry.create_ts = create_timestamp_ms;
        entry.remote = remote;
        entry.status = _status;
        for (const auto &kv : _progress) {
            if (!kv.second.is_inited) {
                continue;
            }
            entry.progress[kv.first] = kv.second.stored_decree;
        }
        return entry;
    }

    void report_progress_if_time_up();

    // This function should only be used for testing.
    // Not thread-safe.
    bool is_altering() const { return _is_altering; }

    // Test util
    bool equals_to(const duplication_info &rhs) const { return to_string() == rhs.to_string(); }

    // To json encoded string.
    std::string to_string() const;

private:
    friend class duplication_info_test;
    friend class meta_duplication_service_test;

    error_code do_alter_status(duplication_status::type to_status);

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
    };

    // partition_idx => progress
    std::map<int, partition_progress> _progress;

    uint64_t _last_progress_report_ms{0};

    duplication_status::type _status{duplication_status::DS_INIT};
    duplication_status::type _next_status{duplication_status::DS_INIT};

    struct json_helper
    {
        std::string remote;
        duplication_status::type status;
        int64_t create_timestamp_ms;

        DEFINE_JSON_SERIALIZATION(remote, status, create_timestamp_ms);
    };

public:
    const dupid_t id{0};
    const int32_t app_id{0};
    const std::string remote;
    const std::string store_path; // store path on meta service = get_duplication_path(app, dupid)
    const uint64_t create_timestamp_ms{0}; // the time when this dup is created.
};

extern void json_encode(dsn::json::JsonWriter &out, const duplication_status::type &s);

extern bool json_decode(const dsn::json::JsonObject &in, duplication_status::type &s);

// Macros for writing log message prefixed by appid and dupid.
#define ddebug_dup(_dup_, ...)                                                                     \
    ddebug_f("[a{}d{}] {}", _dup_->app_id, _dup_->id, fmt::format(__VA_ARGS__));
#define dwarn_dup(_dup_, ...)                                                                      \
    dwarn_f("[a{}d{}] {}", _dup_->app_id, _dup_->id, fmt::format(__VA_ARGS__));
#define derror_dup(_dup_, ...)                                                                     \
    derror_f("[a{}d{}] {}", _dup_->app_id, _dup_->id, fmt::format(__VA_ARGS__));
#define dfatal_dup(_dup_, ...)                                                                     \
    dfatal_f("[a{}d{}] {}", _dup_->app_id, _dup_->id, fmt::format(__VA_ARGS__));
#define dassert_dup(_pred_, _dup_, ...)                                                            \
    dassert_f(_pred_, "[a{}d{}] {}", _dup_->app_id, _dup_->id, fmt::format(__VA_ARGS__));

} // namespace replication
} // namespace dsn
