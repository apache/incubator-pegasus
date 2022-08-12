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
#include <dsn/tool-api/gpid.h>
#include "backup_types.h"
#include <dsn/cpp/rpc_holder.h>

namespace dsn {
namespace replication {

class cold_backup_constant
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

} // namespace replication
} // namespace dsn
