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

#include <gtest/gtest_prod.h>
#include <stdint.h>
#include <map>
#include <string>

#include "backup_types.h"
#include "common/json_helper.h"
#include "task/task_tracker.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
class blob;
class gpid;
class host_port;

namespace dist {
namespace block_service {
class block_filesystem;
} // namespace block_service
} // namespace dist

namespace replication {

enum backup_status
{
    UNALIVE = 1,
    ALIVE = 2,
    COMPLETED = 3,
    FAILED = 4
};

struct app_backup_info
{
    int64_t backup_id;
    int64_t start_time_ms;
    int64_t end_time_ms;

    int32_t app_id;
    std::string app_name;

    app_backup_info() : backup_id(0), start_time_ms(0), end_time_ms(0) {}

    DEFINE_JSON_SERIALIZATION(backup_id, start_time_ms, end_time_ms, app_id, app_name)
};

class backup_service;

class backup_engine
{
public:
    backup_engine(backup_service *service);
    ~backup_engine();

    error_code init_backup(int32_t app_id);
    error_code set_block_service(const std::string &provider);
    error_code set_backup_path(const std::string &path);

    error_code start();

    int64_t get_current_backup_id() const { return _cur_backup.backup_id; }
    int32_t get_backup_app_id() const { return _cur_backup.app_id; }
    bool is_in_progress() const;

    backup_item get_backup_item() const;

private:
    friend class backup_engine_test;
    friend class backup_service_test;

    FRIEND_TEST(backup_engine_test, test_on_backup_reply);
    FRIEND_TEST(backup_engine_test, test_backup_completed);
    FRIEND_TEST(backup_engine_test, test_write_backup_info_failed);

    error_code write_backup_file(const std::string &file_name, const dsn::blob &write_buffer);
    error_code backup_app_meta();
    void backup_app_partition(const gpid &pid);
    void on_backup_reply(error_code err,
                         const backup_response &response,
                         gpid pid,
                         const host_port &primary);
    void write_backup_info();
    void complete_current_backup();
    void handle_replica_backup_failed(const backup_response &response, const gpid pid);
    void retry_backup(const dsn::gpid pid);

    const std::string get_policy_name() const
    {
        return "fake_policy_" + std::to_string(_cur_backup.backup_id);
    }

    backup_service *_backup_service;
    dist::block_service::block_filesystem *_block_service;
    std::string _backup_path;
    std::string _provider_type;
    dsn::task_tracker _tracker;

    // lock the following variables.
    mutable dsn::zlock _lock;
    bool _is_backup_failed;
    app_backup_info _cur_backup;
    // partition_id -> backup_status
    std::map<int32_t, backup_status> _backup_status;
};

} // namespace replication
} // namespace dsn
