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
#include <dsn/http/http_server.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/output_utils.h>
#include <dsn/utils/time_utils.h>

#include "block_service/block_service_manager.h"
#include "common/backup_common.h"
#include "meta_backup_service.h"
#include "meta_service.h"
#include "server_state.h"

namespace dsn {
namespace replication {

backup_service::backup_service(meta_service *meta_svc,
                               const std::string &policy_meta_root,
                               const std::string &backup_root)
    : _meta_svc(meta_svc), _policy_meta_root(policy_meta_root), _backup_root(backup_root)
{
    _state = _meta_svc->get_server_state();
}

// TODO(heyuchen): implement it
void backup_service::start() {}

// TODO(heyuchen): implement it
void backup_service::start_backup_app(start_backup_app_rpc rpc) {}

// TODO(heyuchen): implement it
void backup_service::query_backup_status(query_backup_status_rpc rpc) {}

} // namespace replication
} // namespace dsn
