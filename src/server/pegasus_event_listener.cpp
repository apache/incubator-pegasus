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

#include <fmt/core.h>
#include <rocksdb/compaction_job_stats.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/types.h>
#include <string>

#include "common/gpid.h"
#include "perf_counter/perf_counter.h"
#include "utils/fmt_logging.h"

namespace rocksdb {
class DB;
} // namespace rocksdb

namespace pegasus {
namespace server {

pegasus_event_listener::pegasus_event_listener(replica_base *r) : replica_base(r)
{
    _pfc_recent_flush_completed_count.init_app_counter("app.pegasus",
                                                       "recent.flush.completed.count",
                                                       COUNTER_TYPE_VOLATILE_NUMBER,
                                                       "rocksdb recent flush completed count");
    _pfc_recent_flush_output_bytes.init_app_counter("app.pegasus",
                                                    "recent.flush.output.bytes",
                                                    COUNTER_TYPE_VOLATILE_NUMBER,
                                                    "rocksdb recent flush output bytes");
    _pfc_recent_compaction_completed_count.init_app_counter(
        "app.pegasus",
        "recent.compaction.completed.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "rocksdb recent compaction completed count");
    _pfc_recent_compaction_input_bytes.init_app_counter("app.pegasus",
                                                        "recent.compaction.input.bytes",
                                                        COUNTER_TYPE_VOLATILE_NUMBER,
                                                        "rocksdb recent compaction input bytes");
    _pfc_recent_compaction_output_bytes.init_app_counter("app.pegasus",
                                                         "recent.compaction.output.bytes",
                                                         COUNTER_TYPE_VOLATILE_NUMBER,
                                                         "rocksdb recent compaction output bytes");
    _pfc_recent_write_change_delayed_count.init_app_counter(
        "app.pegasus",
        "recent.write.change.delayed.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "rocksdb recent write change delayed count");
    _pfc_recent_write_change_stopped_count.init_app_counter(
        "app.pegasus",
        "recent.write.change.stopped.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "rocksdb recent write change stopped count");

    // replica-level perfcounter
    std::string counter_str = fmt::format("recent_rdb_compaction_input_bytes@{}", r->get_gpid());
    _pfc_recent_rdb_compaction_input_bytes.init_app_counter(
        "app.pegasus",
        counter_str.c_str(),
        COUNTER_TYPE_VOLATILE_NUMBER,
        "rocksdb recent compaction input bytes");

    counter_str = fmt::format("recent_rdb_compaction_output_bytes@{}", r->get_gpid());
    _pfc_recent_rdb_compaction_output_bytes.init_app_counter(
        "app.pegasus",
        counter_str.c_str(),
        COUNTER_TYPE_VOLATILE_NUMBER,
        "rocksdb recent compaction output bytes");
}

void pegasus_event_listener::OnFlushCompleted(rocksdb::DB *db,
                                              const rocksdb::FlushJobInfo &flush_job_info)
{
    _pfc_recent_flush_completed_count->increment();
    _pfc_recent_flush_output_bytes->add(flush_job_info.table_properties.data_size);
}

void pegasus_event_listener::OnCompactionCompleted(rocksdb::DB *db,
                                                   const rocksdb::CompactionJobInfo &ci)
{
    _pfc_recent_compaction_completed_count->increment();
    _pfc_recent_compaction_input_bytes->add(ci.stats.total_input_bytes);
    _pfc_recent_compaction_output_bytes->add(ci.stats.total_output_bytes);

    _pfc_recent_rdb_compaction_input_bytes->add(ci.stats.total_input_bytes);
    _pfc_recent_rdb_compaction_output_bytes->add(ci.stats.total_output_bytes);
}

void pegasus_event_listener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info)
{
    if (info.condition.cur == rocksdb::WriteStallCondition::kDelayed) {
        LOG_ERROR_PREFIX("rocksdb write delayed");
        _pfc_recent_write_change_delayed_count->increment();
    } else if (info.condition.cur == rocksdb::WriteStallCondition::kStopped) {
        LOG_ERROR_PREFIX("rocksdb write stopped");
        _pfc_recent_write_change_stopped_count->increment();
    }
}

} // namespace server
} // namespace pegasus
