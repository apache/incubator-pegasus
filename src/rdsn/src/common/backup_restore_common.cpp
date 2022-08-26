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
#include <dsn/utility/filesystem.h>

#include "backup_restore_common.h"

namespace dsn {
namespace replication {

DSN_DEFINE_string("replication",
                  cold_backup_root,
                  "",
                  "cold backup remote block service storage path prefix");

const std::string backup_constant::APP_METADATA("app_metadata");
const std::string backup_constant::APP_BACKUP_STATUS("app_backup_status");
const std::string backup_constant::CURRENT_CHECKPOINT("current_checkpoint");
const std::string backup_constant::BACKUP_METADATA("backup_metadata");
const std::string backup_constant::BACKUP_INFO("backup_info");
const int32_t backup_constant::PROGRESS_FINISHED = 1000;
const std::string backup_constant::DATA_VERSION("data_version");

const std::string backup_restore_constant::FORCE_RESTORE("restore.force_restore");
const std::string backup_restore_constant::BLOCK_SERVICE_PROVIDER("restore.block_service_provider");
const std::string backup_restore_constant::CLUSTER_NAME("restore.cluster_name");
const std::string backup_restore_constant::POLICY_NAME("restore.policy_name");
const std::string backup_restore_constant::APP_NAME("restore.app_name");
const std::string backup_restore_constant::APP_ID("restore.app_id");
const std::string backup_restore_constant::BACKUP_ID("restore.backup_id");
const std::string backup_restore_constant::SKIP_BAD_PARTITION("restore.skip_bad_partition");
const std::string backup_restore_constant::RESTORE_PATH("restore.restore_path");

std::string get_backup_root(const std::string &backup_root,
                            const std::string &user_defined_root_path)
{
    if (user_defined_root_path.empty()) {
        return backup_root;
    }
    return utils::filesystem::path_combine(user_defined_root_path, backup_root);
}

std::string get_backup_path(const std::string &root,
                            const std::string &app_name,
                            const int32_t app_id,
                            const int64_t backup_id,
                            const bool is_compatible)
{
    std::string str_app = fmt::format("{}_{}", app_name, app_id);
    if (!is_compatible) {
        return fmt::format("{}/{}/{}", root, str_app, backup_id);
    } else {
        return fmt::format("{}/{}/{}", root, backup_id, str_app);
    }
}

std::string get_backup_meta_path(const std::string &root,
                                 const std::string &app_name,
                                 const int32_t app_id,
                                 const int64_t backup_id,
                                 const bool is_compatible)
{
    return fmt::format("{}/meta",
                       get_backup_path(root, app_name, app_id, backup_id, is_compatible));
}

std::string get_backup_partition_path(const std::string &root,
                                      const std::string &app_name,
                                      const int32_t app_id,
                                      const int64_t backup_id,
                                      const int32_t pidx,
                                      const bool is_compatible)
{
    return fmt::format(
        "{}/{}", get_backup_path(root, app_name, app_id, backup_id, is_compatible), pidx);
}

std::string get_checkpoint_str()
{
    auto node_address = dsn_primary_address();
    return fmt::format("chkpt_{}_{}", node_address.ipv4_str(), node_address.port());
}

std::string get_backup_checkpoint_path(const std::string &partition_path)
{
    return fmt::format("{}/{}", partition_path, get_checkpoint_str());
}

} // namespace replication
} // namespace dsn
