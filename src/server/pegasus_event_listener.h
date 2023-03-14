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

#include <rocksdb/listener.h>

#include "perf_counter/perf_counter_wrapper.h"
#include "replica/replica_base.h"

namespace rocksdb {
class DB;
} // namespace rocksdb

namespace pegasus {
namespace server {

class pegasus_event_listener : public rocksdb::EventListener, dsn::replication::replica_base
{
public:
    explicit pegasus_event_listener(replica_base *r);
    ~pegasus_event_listener() override = default;

    void OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info) override;

    void OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) override;

    void OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) override;

private:
    ::dsn::perf_counter_wrapper _pfc_recent_flush_completed_count;
    ::dsn::perf_counter_wrapper _pfc_recent_flush_output_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_compaction_completed_count;
    ::dsn::perf_counter_wrapper _pfc_recent_compaction_input_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_compaction_output_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_write_change_delayed_count;
    ::dsn::perf_counter_wrapper _pfc_recent_write_change_stopped_count;

    // replica-level perfcounter
    ::dsn::perf_counter_wrapper _pfc_recent_rdb_compaction_input_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_rdb_compaction_output_bytes;
};

} // namespace server
} // namespace pegasus
