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

#include "replica/replica_base.h"
#include "meta_admin_types.h"
#include "partition_split_types.h"
#include "duplication_types.h"
#include "bulk_load_types.h"
#include "backup_types.h"
#include "consensus_types.h"
#include "replica_admin_types.h"

namespace dsn {
namespace replication {

class replica;
class replica_backup_manager : replica_base
{
public:
    explicit replica_backup_manager(replica *r);
    ~replica_backup_manager();

    void on_clear_cold_backup(const backup_clear_request &request);
    void start_collect_backup_info();

private:
    void clear_backup_checkpoint(const std::string &policy_name);
    void send_clear_request_to_secondaries(const gpid &pid, const std::string &policy_name);
    void background_clear_backup_checkpoint(const std::string &policy_name);
    void collect_backup_info();

    replica *_replica;
    dsn::task_ptr _collect_info_timer;

    friend class replica;
    friend class replica_backup_manager_test;
};

} // namespace replication
} // namespace dsn
