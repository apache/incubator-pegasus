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

#include <dsn/cpp/json_helper.h>
#include <dsn/dist/block_service.h>
#include <dsn/tool-api/zlocks.h>

namespace dsn {
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

class app_state;
class backup_service;

class backup_engine
{
public:
    backup_engine(backup_service *service);
    ~backup_engine();

    error_code init_backup(int32_t app_id);
    error_code set_block_service(const std::string &provider);

    error_code start();

    int64_t get_current_backup_id() const { return _cur_backup.backup_id; }
    int32_t get_backup_app_id() const { return _cur_backup.app_id; }
    bool is_backing_up();

private:
    error_code get_app_stat(int32_t app_id, std::shared_ptr<app_state> &app);

    backup_service *_backup_service;
    dist::block_service::block_filesystem *_block_service;
    std::string _provider_type;
    dsn::task_tracker _tracker;

    // lock the following variables.
    dsn::zlock _lock;
    bool is_backup_failed;
    app_backup_info _cur_backup;
    // partition_id -> backup_status
    std::map<int32_t, backup_status> _backup_status;
};

} // namespace replication
} // namespace dsn
