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

#include <absl/strings/string_view.h>

#include "replica/replica.h"
#include "utils/flags.h"
#include "utils/metrics.h"

METRIC_DEFINE_gauge_int64(replica,
                          backup_running_count,
                          dsn::metric_unit::kBackups,
                          "The number of current running backups");

METRIC_DEFINE_gauge_int64(replica,
                          backup_max_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The max backup duration among backups");

METRIC_DEFINE_gauge_int64(replica,
                          backup_file_upload_max_bytes,
                          dsn::metric_unit::kBytes,
                          "The max size of uploaded files among backups");

DSN_DECLARE_int32(cold_backup_checkpoint_reserve_minutes);
DSN_DECLARE_int32(gc_interval_ms);

namespace dsn {
namespace replication {

// TODO(heyuchen): implement it

replica_backup_manager::replica_backup_manager(replica *r) : replica_base(r), _replica(r) {}

replica_backup_manager::~replica_backup_manager() {}

} // namespace replication
} // namespace dsn
