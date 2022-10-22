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

#include <cstdio>
#include <sstream>
#include <iomanip> // std::setfill, std::setw
#include <functional>

#include "block_service/block_service.h"
#include "http/http_server.h"
#include "perf_counter/perf_counter_wrapper.h"
#include <gtest/gtest_prod.h>

#include "backup_engine.h"
#include "meta_data.h"
#include "meta_rpc_types.h"

namespace dsn {
namespace replication {

class meta_service;
class server_state;
class backup_service;

struct backup_info_status
{
    enum type
    {
        ALIVE = 1, // backup info is preserved

        DELETING = 2 // backup info is under deleting, should check whether backup checkpoint is
                     // fully removed on backup media, then remove the backup_info on remote storage
    };
};

struct backup_info
{
    int64_t backup_id;
    int64_t start_time_ms;
    int64_t end_time_ms;

    // "app_ids" is copied from policy.app_ids when
    // a new backup is generated. The policy's
    // app set may be changed, but backup_info.app_ids
    // never change.
    std::set<int32_t> app_ids;
    std::map<int32_t, std::string> app_names;
    int32_t info_status;
    backup_info_status::type get_backup_status() const
    {
        return backup_info_status::type(info_status);
    }
    backup_info()
        : backup_id(0), start_time_ms(0), end_time_ms(0), info_status(backup_info_status::ALIVE)
    {
    }
    DEFINE_JSON_SERIALIZATION(
        backup_id, start_time_ms, end_time_ms, app_ids, app_names, info_status)
};

// Attention: backup_start_time == 24:00 is represent no limit for start_time, 24:00 is mainly saved
// for testing
//
// current, we don't support accurating to minute, only support accurating to hour, so
// we just set minute to 0
struct backup_start_time
{
    int32_t hour;   // [0 ~24)
    int32_t minute; // [0 ~ 60)
    backup_start_time() : hour(0), minute(0) {}
    backup_start_time(int32_t h, int32_t m) : hour(h), minute(m) {}
    std::string to_string() const
    {
        std::stringstream ss;
        ss << std::setw(2) << std::setfill('0') << std::to_string(hour) << ":" << std::setw(2)
           << std::setfill('0') << std::to_string(minute);
        return ss.str();
    }
    // NOTICE: this function will modify hour and minute, if time is invalid, this func will set
    // hour = 24, minute = 0
    bool parse_from(const std::string &time)
    {
        if (::sscanf(time.c_str(), "%d:%d", &hour, &minute) != 2) {
            return false;
        } else {
            if (hour > 24) {
                hour = 24;
                minute = 0;
                return false;
            }

            if (hour == 24 && minute != 0) {
                minute = 0;
                return false;
            }

            if (minute >= 60) {
                hour = 24;
                minute = 0;
                return false;
            }
        }
        return true;
    }

    // return the interval between new_hour:new_min and start_time,
    // namely new_hour:new_min - start_time;
    // unit is ms
    int64_t compute_time_drift_ms(int32_t new_hour, int32_t new_min)
    {
        int64_t res = 0;
        // unit is hour
        res += (new_hour - hour);
        // unit is minute
        res *= 60;
        res += (new_min - minute);
        // unit is ms
        return (res * 60 * 1000);
    }

    // judge whether we should start backup base current time
    bool should_start_backup(int32_t cur_hour, int32_t cur_min)
    {
        if (hour == 24) {
            // erase the restrict of backup_start_time, just for testing
            return true;
        }
        // NOTICE : if you want more precisely, you can use cur_min to implement
        // now, we just ignore
        return (cur_hour == hour);
    }
    DEFINE_JSON_SERIALIZATION(hour, minute)
};

//
// the backup process of meta server:
//      1, write the app metadata to block filesystem
//      2, tell the primary of each partition periodically to start backup until app finish backup
//      3, receive the backup response from each primary to judge whether backup is finished
//      4, if one app finish its backup, write a flag to block filesystem(we write a file named
//         app_backup_status to represent the flag) to represent it has finished backup
//      5, if policy finished backup, write the backup information (backup_info) to block filesystem
//      6, backup is finished, we just wait to start another backup
//

class policy : public policy_info
{
public:
    std::set<int32_t> app_ids;
    std::map<int32_t, std::string> app_names;
    int64_t backup_interval_seconds;
    int32_t backup_history_count_to_keep;
    bool is_disable;
    backup_start_time start_time;
    policy()
        : app_ids(),
          backup_interval_seconds(0),
          backup_history_count_to_keep(6),
          is_disable(false),
          start_time(24, 0) // default is 24:00, namely no limit
    {
    }

    DEFINE_JSON_SERIALIZATION(policy_name,
                              backup_provider_type,
                              app_ids,
                              app_names,
                              backup_interval_seconds,
                              backup_history_count_to_keep,
                              is_disable,
                              start_time)
};

struct backup_progress
{
    int32_t unfinished_apps;
    std::map<gpid, int32_t> partition_progress;
    std::map<app_id, int32_t> unfinished_partitions_per_app;
    // <app_id, <partition_id, checkpoint size>>
    std::map<app_id, std::map<int, int64_t>> app_chkpt_size;
    // if app is dropped when starting a new backup or under backuping, we just skip backup this app
    std::map<app_id, bool> is_app_skipped;

    backup_progress() : unfinished_apps(0) {}

    void reset()
    {
        unfinished_apps = 0;
        partition_progress.clear();
        unfinished_partitions_per_app.clear();
        app_chkpt_size.clear();
        is_app_skipped.clear();
    }
};

struct backup_flag
{
    int64_t total_checkpoint_size;
    DEFINE_JSON_SERIALIZATION(total_checkpoint_size)
};

class policy_context
{
public:
    explicit policy_context(backup_service *service)
        : _backup_service(service), _block_service(nullptr)
    {
    }
    mock_virtual ~policy_context() {}

    void set_policy(const policy &p);
    policy get_policy();
    void add_backup_history(const backup_info &info);
    std::vector<backup_info> get_backup_infos(int cnt);
    bool is_under_backuping();
    mock_virtual void start();
    // function above will called be others, before call these function, should lock the _lock of
    // policy_context, otherwise maybe lead deadlock

    // clang-format off
mock_private :
    //
    // update the partition progress
    // the progress for app and whole-backup-instance will also updated accordingly.
    // if whole-backup-instance is finished, sync it to the remote storage.
    // NOTICE: the local "_cur_backup" is reset after it is successfully synced to remote,
    //   which is in another task.
    // so user can safely visit "_cur_backup" after this function call,
    //   as long as the _lock is held.
    //
    // Return: true if the partition is finished, or-else false
    //

    mock_virtual bool
    update_partition_progress_unlocked(gpid pid, int32_t progress, const rpc_address &source);
    mock_virtual void record_partition_checkpoint_size_unlock(const gpid& pid, int64_t size);

    mock_virtual void start_backup_app_meta_unlocked(int32_t app_id);
    mock_virtual void start_backup_app_partitions_unlocked(int32_t app_id);
    mock_virtual void start_backup_partition_unlocked(gpid pid);
    // before finish backup one app, we write a flag file to represent whether the app's backup is
    // finished
    mock_virtual void write_backup_app_finish_flag_unlocked(int32_t app_id,
                                                            dsn::task_ptr write_callback);
    mock_virtual void finish_backup_app_unlocked(int32_t app_id);
    // after finish backup all app, we record the information of policy's backup to block filesystem
    mock_virtual void write_backup_info_unlocked(const backup_info &b_info,
                                                 dsn::task_ptr write_callback);

    mock_virtual void sync_backup_to_remote_storage_unlocked(const backup_info &b_info,
                                                             dsn::task_ptr sync_callback,
                                                             bool create_new_node);
    mock_virtual void initialize_backup_progress_unlocked();
    mock_virtual void prepare_current_backup_on_new_unlocked();
    mock_virtual void issue_new_backup_unlocked();
    // returns:
    //  - true, should start backup right now, otherwise don't start backup
    mock_virtual bool should_start_backup_unlocked();
    mock_virtual void continue_current_backup_unlocked();

    mock_virtual void on_backup_reply(dsn::error_code err,
                                      backup_response &&response,
                                      gpid pid,
                                      const rpc_address &primary);

    mock_virtual void gc_backup_info_unlocked(const backup_info &info_to_gc);
    mock_virtual void issue_gc_backup_info_task_unlocked();
    mock_virtual void sync_remove_backup_info(const backup_info &info, dsn::task_ptr sync_callback);

mock_private :
    friend class backup_service;
    backup_service *_backup_service;

    // lock the data-structure below
    dsn::zlock _lock;

    // policy related
    policy _policy;
    dist::block_service::block_filesystem *_block_service;

    // backup related
    backup_info _cur_backup;
    bool _is_backup_failed;
    // backup_id --> backup_info
    std::map<int64_t, backup_info> _backup_history;
    backup_progress _progress;
    std::string _backup_sig; // policy_name@backup_id, used when print backup related log

    perf_counter_wrapper _counter_policy_recent_backup_duration_ms;
//clang-format on
    dsn::task_tracker _tracker;
};

class backup_service
{
public:
    struct backup_opt
    {
        std::chrono::milliseconds meta_retry_delay_ms;
        std::chrono::milliseconds block_retry_delay_ms;
        std::chrono::milliseconds app_dropped_retry_delay_ms;
        std::chrono::milliseconds reconfiguration_retry_delay_ms;
        std::chrono::milliseconds request_backup_period_ms; // period that meta send backup command to replica
        std::chrono::milliseconds issue_backup_interval_ms; // interval that meta try to issue a new backup
    };

    typedef std::function<std::shared_ptr<policy_context>(backup_service *)> policy_factory;
    explicit backup_service(meta_service *meta_svc,
                            const std::string &policy_meta_root,
                            const std::string &backup_root,
                            const policy_factory &factory);
    meta_service *get_meta_service() const { return _meta_svc; }
    server_state *get_state() const { return _state; }
    backup_opt &backup_option() { return _opt; }
    void start();

    const std::string &backup_root() const { return _backup_root; }
    const std::string &policy_root() const { return _policy_meta_root; }
    void add_backup_policy(dsn::message_ex* msg);
    void query_backup_policy(query_backup_policy_rpc rpc);
    void modify_backup_policy(configuration_modify_backup_policy_rpc rpc);
    void start_backup_app(start_backup_app_rpc rpc);
    void query_backup_status(query_backup_status_rpc rpc);

    // compose the absolute path(AP) for policy
    // input:
    //  -- root:        the prefix of the AP
    // return:
    //      the AP of this policy: <policy_meta_root>/<policy_name>
    std::string get_policy_path(const std::string &policy_name);
    // compose the absolute path(AP) for backup
    // input:
    //  -- root:        the prefix of the AP
    // return:
    //      the AP of this backup: <policy_meta_root>/<policy_name>/<backup_id>
    std::string get_backup_path(const std::string &policy_name, int64_t backup_id);

private:
    friend class backup_service_test;
    friend class meta_service_test_app;

    FRIEND_TEST(backup_service_test, test_init_backup);
    FRIEND_TEST(backup_service_test, test_query_backup_status);
    FRIEND_TEST(meta_backup_service_test, test_add_backup_policy);

    void start_create_policy_meta_root(dsn::task_ptr callback);
    void start_sync_policies();
    error_code sync_policies_from_remote_storage();

    void do_add_policy(dsn::message_ex* req,
                       std::shared_ptr<policy_context> p,
                       const std::string &hint_msg);
    void do_update_policy_to_remote_storage(configuration_modify_backup_policy_rpc rpc,
                                            const policy &p,
                                            std::shared_ptr<policy_context> &p_context_ptr);

    bool is_valid_policy_name_unlocked(const std::string &policy_name);

    policy_factory _factory;
    meta_service *_meta_svc;
    server_state *_state;

    // lock _policy_states and _backup_states.
    zlock _lock;
    std::map<std::string, std::shared_ptr<policy_context>>
        _policy_states; // policy_name -> policy_context

    // _backup_states stores all states of one-time backup in the cluster, not persistence to ZK
    std::vector<std::shared_ptr<backup_engine>> _backup_states;

    // the root of policy metas, stored on remote_storage(zookeeper)
    std::string _policy_meta_root;
    // the root of cold backup data, stored on block service
    std::string _backup_root;

    backup_opt _opt;
    std::atomic_bool _in_initialize;
    dsn::task_tracker _tracker;
};
} // namespace replication
} // namespace dsn
