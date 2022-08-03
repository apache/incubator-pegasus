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

#include "replica_backup_manager.h"
#include "replica/replica.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

// TODO(heyuchen): implement it

replica_backup_manager::replica_backup_manager(replica *r) : replica_base(r), _replica(r) {}

replica_backup_manager::~replica_backup_manager() {}

} // namespace replication
} // namespace dsn
