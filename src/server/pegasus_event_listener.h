// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/listener.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>

namespace pegasus {
namespace server {

class pegasus_event_listener : public rocksdb::EventListener
{
public:
    pegasus_event_listener();
    virtual ~pegasus_event_listener();

    virtual void OnFlushCompleted(rocksdb::DB *db,
                                  const rocksdb::FlushJobInfo &flush_job_info) override;

    virtual void OnCompactionCompleted(rocksdb::DB *db,
                                       const rocksdb::CompactionJobInfo &ci) override;

    virtual void OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) override;

private:
    ::dsn::perf_counter_wrapper _pfc_recent_flush_completed_count;
    ::dsn::perf_counter_wrapper _pfc_recent_flush_output_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_compaction_completed_count;
    ::dsn::perf_counter_wrapper _pfc_recent_compaction_input_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_compaction_output_bytes;
    ::dsn::perf_counter_wrapper _pfc_recent_write_change_delayed_count;
    ::dsn::perf_counter_wrapper _pfc_recent_write_change_stopped_count;
};

} // namespace server
} // namespace pegasus
