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

#include <dsn/dist/fmt_logging.h>

#include "common/backup_utils.h"
#include "server_state.h"

namespace dsn {
namespace replication {

backup_engine::backup_engine(backup_service *service)
    : _backup_service(service), _block_service(nullptr), is_backup_failed(false)
{
}

backup_engine::~backup_engine() { _tracker.cancel_outstanding_tasks(); }

error_code backup_engine::get_app_stat(int32_t app_id, std::shared_ptr<app_state> &app)
{
    zauto_read_lock l;
    _backup_service->get_state()->lock_read(l);
    app = _backup_service->get_state()->get_app(app_id);
    if (app == nullptr || app->status != app_status::AS_AVAILABLE) {
        derror_f("app {} is not available, couldn't do backup now.", app_id);
        return ERR_INVALID_STATE;
    }
    return ERR_OK;
}

error_code backup_engine::init_backup(int32_t app_id)
{
    std::shared_ptr<app_state> app;
    error_code err = get_app_stat(app_id, app);
    if (err != ERR_OK) {
        return err;
    }

    zauto_lock lock(_lock);
    _backup_status.clear();
    for (int i = 0; i < app->partition_count; ++i) {
        _backup_status.emplace(i, backup_status::UNALIVE);
    }
    _cur_backup.app_id = app_id;
    _cur_backup.app_name = app->app_name;
    _cur_backup.backup_id = static_cast<int64_t>(dsn_now_ms());
    _cur_backup.start_time_ms = _cur_backup.backup_id;
    return ERR_OK;
}

error_code backup_engine::set_block_service(const std::string &provider)
{
    _provider_type = provider;
    _block_service = _backup_service->get_meta_service()
                         ->get_block_service_manager()
                         .get_or_create_block_filesystem(provider);
    if (_block_service == nullptr) {
        return ERR_INVALID_PARAMETERS;
    }
    return ERR_OK;
}

error_code backup_engine::start() { return ERR_OK; }

bool backup_engine::is_backing_up()
{
    zauto_lock l(_lock);
    return _cur_backup.end_time_ms == 0 && !is_backup_failed;
}

} // namespace replication
} // namespace dsn
