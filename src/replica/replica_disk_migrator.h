/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <string>

#include "replica/replica_base.h"
#include "replica/replica_stub.h"
#include "replica_admin_types.h"
#include "task/task.h"

namespace dsn {
namespace replication {
class replica;

class replica_disk_migrator : replica_base
{
public:
    explicit replica_disk_migrator(replica *r);
    ~replica_disk_migrator();

    void on_migrate_replica(replica_disk_migrate_rpc rpc);

    disk_migration_status::type status() const { return _status; }

    void set_status(const disk_migration_status::type &status) { _status = status; }

private:
    bool check_migration_args(replica_disk_migrate_rpc rpc);

    void migrate_replica(const replica_disk_migrate_request &req);

    bool init_target_dir(const replica_disk_migrate_request &req);
    bool migrate_replica_checkpoint(const replica_disk_migrate_request &req);
    bool migrate_replica_app_info(const replica_disk_migrate_request &req);
    /// return nullptr if close failed. The returned value is only used in unit-tests.
    dsn::task_ptr close_current_replica(const replica_disk_migrate_request &req);
    void update_replica_dir();

    void reset_status() { _status = disk_migration_status::IDLE; }

private:
    const static std::string kReplicaDirTempSuffix;
    const static std::string kReplicaDirOriginSuffix;
    const static std::string kDataDirFolder;

    replica *_replica;

    std::string _target_replica_dir; // /root/ssd_tag/gpid.pegasus/
    std::string _target_data_dir;    // /root/ssd_tag/gpid.pegasus/data/rdb
    disk_migration_status::type _status{disk_migration_status::IDLE};

    friend class replica;
    friend class replica_stub;
    friend class replica_disk_migrate_test;
};

} // namespace replication
} // namespace dsn
