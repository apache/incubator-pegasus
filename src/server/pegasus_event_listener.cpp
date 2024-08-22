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

#include "pegasus_event_listener.h"

#include <string_view>
#include <rocksdb/compaction_job_stats.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/types.h>

#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"

METRIC_DEFINE_counter(replica,
                      rdb_flush_completed_count,
                      dsn::metric_unit::kFlushes,
                      "The number of completed rocksdb flushes");

METRIC_DEFINE_counter(replica,
                      rdb_flush_output_bytes,
                      dsn::metric_unit::kBytes,
                      "The size of rocksdb flush output in bytes");

METRIC_DEFINE_counter(replica,
                      rdb_compaction_completed_count,
                      dsn::metric_unit::kCompactions,
                      "The number of completed rocksdb compactions");

METRIC_DEFINE_counter(replica,
                      rdb_compaction_input_bytes,
                      dsn::metric_unit::kBytes,
                      "The size of rocksdb compaction input in bytes");

METRIC_DEFINE_counter(replica,
                      rdb_compaction_output_bytes,
                      dsn::metric_unit::kBytes,
                      "The size of rocksdb compaction output in bytes");

METRIC_DEFINE_counter(
    replica,
    rdb_changed_delayed_writes,
    dsn::metric_unit::kWrites,
    "The number of rocksdb delayed writes changed from another write stall condition");

METRIC_DEFINE_counter(
    replica,
    rdb_changed_stopped_writes,
    dsn::metric_unit::kWrites,
    "The number of rocksdb stopped writes changed from another write stall condition");

namespace rocksdb {
class DB;
} // namespace rocksdb

namespace pegasus {
namespace server {

pegasus_event_listener::pegasus_event_listener(replica_base *r)
    : replica_base(r),
      METRIC_VAR_INIT_replica(rdb_flush_completed_count),
      METRIC_VAR_INIT_replica(rdb_flush_output_bytes),
      METRIC_VAR_INIT_replica(rdb_compaction_completed_count),
      METRIC_VAR_INIT_replica(rdb_compaction_input_bytes),
      METRIC_VAR_INIT_replica(rdb_compaction_output_bytes),
      METRIC_VAR_INIT_replica(rdb_changed_delayed_writes),
      METRIC_VAR_INIT_replica(rdb_changed_stopped_writes)
{
}

void pegasus_event_listener::OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &info)
{
    METRIC_VAR_INCREMENT(rdb_flush_completed_count);
    METRIC_VAR_INCREMENT_BY(rdb_flush_output_bytes, info.table_properties.data_size);
}

void pegasus_event_listener::OnCompactionCompleted(rocksdb::DB *db,
                                                   const rocksdb::CompactionJobInfo &info)
{
    METRIC_VAR_INCREMENT(rdb_compaction_completed_count);
    METRIC_VAR_INCREMENT_BY(rdb_compaction_input_bytes, info.stats.total_input_bytes);
    METRIC_VAR_INCREMENT_BY(rdb_compaction_output_bytes, info.stats.total_output_bytes);
}

void pegasus_event_listener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info)
{
    if (info.condition.cur == rocksdb::WriteStallCondition::kDelayed) {
        LOG_ERROR_PREFIX("rocksdb write delayed");
        METRIC_VAR_INCREMENT(rdb_changed_delayed_writes);
    } else if (info.condition.cur == rocksdb::WriteStallCondition::kStopped) {
        LOG_ERROR_PREFIX("rocksdb write stopped");
        METRIC_VAR_INCREMENT(rdb_changed_stopped_writes);
    }
}

} // namespace server
} // namespace pegasus
