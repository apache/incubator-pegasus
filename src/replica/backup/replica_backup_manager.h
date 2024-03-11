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

#include "replica/replica_base.h"
#include "runtime/task/task.h"
#include "utils/metrics.h"

namespace dsn {
class gpid;

namespace replication {

// TODO(heyuchen): implement it

class replica;

class replica_backup_manager : replica_base
{
public:
    explicit replica_backup_manager(replica *r);
    ~replica_backup_manager();

private:
    friend class replica;
    friend class replica_backup_manager_test;

    replica *_replica;
};

} // namespace replication
} // namespace dsn
