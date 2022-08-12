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

#include "backup_restore_common.h"

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

} // namespace replication
} // namespace dsn
