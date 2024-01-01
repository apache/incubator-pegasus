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

#include "backup_common.h"

#include "common/gpid.h"
#include "fmt/core.h"
#include "rpc/rpc_host_port.h"
#include "runtime/api_layer1.h"

namespace dsn {
namespace replication {
const std::string cold_backup_constant::APP_METADATA("app_metadata");
const std::string cold_backup_constant::APP_BACKUP_STATUS("app_backup_status");
const std::string cold_backup_constant::CURRENT_CHECKPOINT("current_checkpoint");
const std::string cold_backup_constant::BACKUP_METADATA("backup_metadata");
const std::string cold_backup_constant::BACKUP_INFO("backup_info");
const int32_t cold_backup_constant::PROGRESS_FINISHED = 1000;

const std::string backup_restore_constant::FORCE_RESTORE("restore.force_restore");
const std::string backup_restore_constant::BLOCK_SERVICE_PROVIDER("restore.block_service_provider");
const std::string backup_restore_constant::CLUSTER_NAME("restore.cluster_name");
const std::string backup_restore_constant::POLICY_NAME("restore.policy_name");
const std::string backup_restore_constant::APP_NAME("restore.app_name");
const std::string backup_restore_constant::APP_ID("restore.app_id");
const std::string backup_restore_constant::BACKUP_ID("restore.backup_id");
const std::string backup_restore_constant::SKIP_BAD_PARTITION("restore.skip_bad_partition");
const std::string backup_restore_constant::RESTORE_PATH("restore.restore_path");

namespace cold_backup {

std::string get_backup_path(const std::string &root, int64_t backup_id)
{
    return root + "/" + std::to_string(backup_id);
}

std::string get_backup_info_file(const std::string &root, int64_t backup_id)
{
    return get_backup_path(root, backup_id) + "/" + cold_backup_constant::BACKUP_INFO;
}

std::string get_replica_backup_path(const std::string &root,
                                    const std::string &app_name,
                                    gpid pid,
                                    int64_t backup_id)
{
    std::string str_app = app_name + "_" + std::to_string(pid.get_app_id());
    return get_backup_path(root, backup_id) + "/" + str_app + "/" +
           std::to_string(pid.get_partition_index());
}

std::string get_app_meta_backup_path(const std::string &root,
                                     const std::string &app_name,
                                     int32_t app_id,
                                     int64_t backup_id)
{
    std::string str_app = app_name + "_" + std::to_string(app_id);
    return get_backup_path(root, backup_id) + "/" + str_app + "/meta";
}

std::string get_app_metadata_file(const std::string &root,
                                  const std::string &app_name,
                                  int32_t app_id,
                                  int64_t backup_id)
{
    return get_app_meta_backup_path(root, app_name, app_id, backup_id) + "/" +
           cold_backup_constant::APP_METADATA;
}

std::string get_app_backup_status_file(const std::string &root,
                                       const std::string &app_name,
                                       int32_t app_id,
                                       int64_t backup_id)
{
    return get_app_meta_backup_path(root, app_name, app_id, backup_id) + "/" +
           cold_backup_constant::APP_BACKUP_STATUS;
}

std::string get_current_chkpt_file(const std::string &root,
                                   const std::string &app_name,
                                   gpid pid,
                                   int64_t backup_id)
{
    return get_replica_backup_path(root, app_name, pid, backup_id) + "/" +
           cold_backup_constant::CURRENT_CHECKPOINT;
}

std::string get_remote_chkpt_dirname()
{
    return fmt::format(
        "chkpt_{}_{}", dsn_primary_host_port().host(), dsn_primary_host_port().port());
}

std::string get_remote_chkpt_dir(const std::string &root,
                                 const std::string &app_name,
                                 gpid pid,
                                 int64_t backup_id)
{
    return get_replica_backup_path(root, app_name, pid, backup_id) + "/" +
           get_remote_chkpt_dirname();
}

std::string get_remote_chkpt_meta_file(const std::string &root,
                                       const std::string &app_name,
                                       gpid pid,
                                       int64_t backup_id)
{
    return get_remote_chkpt_dir(root, app_name, pid, backup_id) + "/" +
           cold_backup_constant::BACKUP_METADATA;
}

} // namespace cold_backup
} // namespace replication
} // namespace dsn
