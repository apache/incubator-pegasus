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

#include "backup_utils.h"
#include "replica/backup/cold_backup_context.h"

namespace dsn {
namespace replication {
const std::string cold_backup_constant::APP_METADATA("app_metadata");
const std::string cold_backup_constant::APP_BACKUP_STATUS("app_backup_status");
const std::string cold_backup_constant::CURRENT_CHECKPOINT("current_checkpoint");
const std::string cold_backup_constant::BACKUP_METADATA("backup_metadata");
const std::string cold_backup_constant::BACKUP_INFO("backup_info");
const int32_t cold_backup_constant::PROGRESS_FINISHED = 1000;

namespace cold_backup {

std::string get_policy_path(const std::string &root, const std::string &policy_name)
{
    std::stringstream ss;
    ss << root << "/" << policy_name;
    return ss.str();
}

std::string
get_backup_path(const std::string &root, const std::string &policy_name, int64_t backup_id)
{
    std::stringstream ss;
    ss << get_policy_path(root, policy_name) << "/" << backup_id;
    return ss.str();
}

std::string get_app_backup_path(const std::string &root,
                                const std::string &policy_name,
                                const std::string &app_name,
                                int32_t app_id,
                                int64_t backup_id)
{
    std::stringstream ss;
    ss << get_backup_path(root, policy_name, backup_id) << "/" << app_name << "_" << app_id;
    return ss.str();
}

std::string get_replica_backup_path(const std::string &root,
                                    const std::string &policy_name,
                                    const std::string &app_name,
                                    gpid pid,
                                    int64_t backup_id)
{
    std::stringstream ss;
    ss << get_policy_path(root, policy_name) << "/" << backup_id << "/" << app_name << "_"
       << pid.get_app_id() << "/" << pid.get_partition_index();
    return ss.str();
}

std::string get_app_meta_backup_path(const std::string &root,
                                     const std::string &policy_name,
                                     const std::string &app_name,
                                     int32_t app_id,
                                     int64_t backup_id)
{
    std::stringstream ss;
    ss << get_policy_path(root, policy_name) << "/" << backup_id << "/" << app_name << "_" << app_id
       << "/meta";
    return ss.str();
}

std::string get_app_metadata_file(const std::string &root,
                                  const std::string &policy_name,
                                  const std::string &app_name,
                                  int32_t app_id,
                                  int64_t backup_id)
{
    std::stringstream ss;
    ss << get_app_meta_backup_path(root, policy_name, app_name, app_id, backup_id) << "/"
       << cold_backup_constant::APP_METADATA;
    return ss.str();
}

std::string get_app_backup_status_file(const std::string &root,
                                       const std::string &policy_name,
                                       const std::string &app_name,
                                       int32_t app_id,
                                       int64_t backup_id)
{
    std::stringstream ss;
    ss << get_app_meta_backup_path(root, policy_name, app_name, app_id, backup_id) << "/"
       << cold_backup_constant::APP_BACKUP_STATUS;
    return ss.str();
}

std::string get_current_chkpt_file(const std::string &root,
                                   const std::string &policy_name,
                                   const std::string &app_name,
                                   gpid pid,
                                   int64_t backup_id)
{
    std::stringstream ss;
    ss << get_replica_backup_path(root, policy_name, app_name, pid, backup_id) << "/"
       << cold_backup_constant::CURRENT_CHECKPOINT;
    return ss.str();
}

std::string get_remote_chkpt_dirname()
{
    // here using server address as suffix of remote_chkpt_dirname
    rpc_address local_address = dsn_primary_address();
    std::stringstream ss;
    ss << "chkpt_" << local_address.ipv4_str() << "_" << local_address.port();
    return ss.str();
}

std::string get_remote_chkpt_dir(const std::string &root,
                                 const std::string &policy_name,
                                 const std::string &app_name,
                                 gpid pid,
                                 int64_t backup_id)
{
    std::stringstream ss;
    ss << get_replica_backup_path(root, policy_name, app_name, pid, backup_id) << "/"
       << get_remote_chkpt_dirname();
    return ss.str();
}

std::string get_remote_chkpt_meta_file(const std::string &root,
                                       const std::string &policy_name,
                                       const std::string &app_name,
                                       gpid pid,
                                       int64_t backup_id)
{
    std::stringstream ss;
    ss << get_remote_chkpt_dir(root, policy_name, app_name, pid, backup_id) << "/"
       << cold_backup_constant::BACKUP_METADATA;
    return ss.str();
}

} // namespace cold_backup
} // namespace replication
} // namespace dsn
