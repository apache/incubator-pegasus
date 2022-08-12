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

#include <string>

#include <dsn/cpp/rpc_holder.h>
#include <dsn/tool-api/gpid.h>
#include <dsn/utility/flags.h>

#include "backup_types.h"

namespace dsn {
namespace replication {

DSN_DECLARE_string(cold_backup_root);

class backup_constant
{
public:
    static const std::string APP_METADATA;
    static const std::string APP_BACKUP_STATUS;
    static const std::string CURRENT_CHECKPOINT;
    static const std::string BACKUP_METADATA;
    static const std::string BACKUP_INFO;
    static const int32_t PROGRESS_FINISHED;
};

typedef rpc_holder<backup_request, backup_response> backup_rpc;

class backup_restore_constant
{
public:
    static const std::string FORCE_RESTORE;
    static const std::string BLOCK_SERVICE_PROVIDER;
    static const std::string CLUSTER_NAME;
    static const std::string POLICY_NAME;
    static const std::string APP_NAME;
    static const std::string APP_ID;
    static const std::string BACKUP_ID;
    static const std::string SKIP_BAD_PARTITION;
    static const std::string RESTORE_PATH;
};

/// The directory structure on block service
///
/// (<root> = <user_define_root>/<cluster>)
///
///  <root>/<app_name>_<app_id>/<backup_id>/<pidx>/chkpt_<ip>_<port>/***.sst
///                                        /<pidx>/chkpt_<ip>_<port>/CURRENT
///                                        /<pidx>/chkpt_<ip>_<port>/IDENTITY
///                                        /<pidx>/chkpt_<ip>_<port>/MANIFEST
///                                        /<pidx>/chkpt_<ip>_<port>/OPTIONS
///                                        /<pidx>/chkpt_<ip>_<port>/LOG
///                                        /<pidx>/chkpt_<ip>_<port>/backup_metadata
///                                        /<pidx>/current_checkpoint
///                                        /<pidx>/data_version
///
///  ......other partitions......
///
///  <root>/<app_name>_<app_id>/<backup_id>/meta/app_metadata
///  <root>/<app_name>_<app_id>/<backup_id>/backup_info
///

///
/// The usage of files:
///      1, app_metadata : the metadata of the app, the same with the app's app_info
///      2, backup_metadata : the file to statistic the information of a checkpoint, include all the
///         file's name, size and md5
///      3, current_checkpoint : specifing which checkpoint directory is valid
///      4, data_version: partition data_version
///      5, backup_info : recording the information of this backup

// TODO(heyuchen): add other common functions
// get_compatible_backup_root is only used for restore compatible backup

// The backup root path on block service
// if user_defined_root_path is not empty
// - return <user_defined_root_path>/<backup_root>
// else
// - return <backup_root>
std::string get_backup_root(const std::string &backup_root,
                            const std::string &user_defined_root_path);

// This backup path on block service
// if is_compatible = false (root is the return value of get_backup_root function)
// - return <root>/<app_name>_<app_id>/<backup_id>
// else (only used for restore compatible backup, root is the return value of
// get_compatible_backup_root function)
// - return <root>/<backup_id>/<app_name>_<app_id>
std::string get_backup_path(const std::string &root,
                            const std::string &app_name,
                            const int32_t app_id,
                            const int64_t backup_id,
                            const bool is_compatible = false);

// This backup meta path on block service
// if is_compatible = false (root is the return value of get_backup_root function)
// - return <root>/<app_name>_<app_id>/<backup_id>/meta
// else (only used for restore compatible backup, root is the return value of
// get_compatible_backup_root function)
// - return <root>/<backup_id>/<app_name>_<app_id>/meta
std::string get_backup_meta_path(const std::string &root,
                                 const std::string &app_name,
                                 const int32_t app_id,
                                 const int64_t backup_id,
                                 const bool is_compatible = false);

} // namespace replication
} // namespace dsn
