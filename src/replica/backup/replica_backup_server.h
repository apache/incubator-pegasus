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

#include "common/backup_common.h"

namespace dsn {
namespace replication {

class backup_clear_request;
class replica_stub;

// A server distributes the cold-backup task to the targeted replica.
class replica_backup_server
{
public:
    explicit replica_backup_server(const replica_stub *rs);
    ~replica_backup_server();

private:
    void on_cold_backup(backup_rpc rpc);

    void on_clear_cold_backup(const backup_clear_request &request);

private:
    const replica_stub *_stub;
};

} // namespace replication
} // namespace dsn
