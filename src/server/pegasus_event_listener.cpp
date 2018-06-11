// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_event_listener.h"

namespace pegasus {
namespace server {

pegasus_event_listener::pegasus_event_listener()
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
}

pegasus_event_listener::~pegasus_event_listener() {}

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
}

void pegasus_event_listener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info)
{
    if (info.condition.cur == rocksdb::WriteStallCondition::kDelayed)
        _pfc_recent_write_change_delayed_count->increment();
    else if (info.condition.cur == rocksdb::WriteStallCondition::kStopped)
        _pfc_recent_write_change_stopped_count->increment();
}

} // namespace server
} // namespace pegasus
