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

namespace dsn {
namespace replication {
class replica;

class replica_disk_migrator : replica_base
{
public:
    explicit replica_disk_migrator(replica *r);
    ~replica_disk_migrator();

    void on_migrate_replica(const replica_disk_migrate_request &req,
                            /*out*/ replica_disk_migrate_response &resp);

    disk_migration_status::type status() const { return _status; }

    void set_status(const disk_migration_status::type &status) { _status = status; }

private:
    bool check_migration_args(const replica_disk_migrate_request &req,
                              /*out*/ replica_disk_migrate_response &resp);

    // TODO(jiashuo1)
    void migrate_replica(const replica_disk_migrate_request &req);

    // TODO(jiashuo1)
    void update_replica_dir();

private:
    replica *_replica;

    disk_migration_status::type _status{disk_migration_status::IDLE};

    friend class replica_disk_migrate_test;
};

} // namespace replication
} // namespace dsn
