// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/perf_counter_wrapper.h>
#include <dsn/dist/replication/replica_base.h>

#include "base/pegasus_value_schema.h"
#include "base/pegasus_utils.h"
#include "rrdb/rrdb_types.h"

namespace pegasus {
namespace server {

class pegasus_server_impl;

#define RETURN_NOT_ZERO(err)                                                                       \
    if (dsn_unlikely(err))                                                                         \
        return err;

/// Handle the write requests.
/// As the signatures imply, this class is not responsible for replying the rpc,
/// the caller(pegasus_server_write) should do.
/// \see pegasus::server::pegasus_server_write::on_batched_write_requests
class pegasus_write_service
{
public:
    explicit pegasus_write_service(pegasus_server_impl *server);

    ~pegasus_write_service();

    int multi_put(int64_t decree,
                  const dsn::apps::multi_put_request &update,
                  dsn::apps::update_response &resp);

    int multi_remove(int64_t decree,
                     const dsn::apps::multi_remove_request &update,
                     dsn::apps::multi_remove_response &resp);

    /// Prepare for batch write.
    void batch_prepare(int64_t decree);

    // NOTE: A batch write may incur a database read for consistency check of timetag.
    // (see pegasus::pegasus_value_generator::generate_value_v1 for more info about timetag)
    // To disable the consistency check, unset `verify_timetag` under `pegasus.server` section
    // in configuration.

    /// Add PUT op in batch.
    /// \returns 0 if success, non-0 if failure.
    /// NOTE that `resp` should not be moved or freed while the batch is not committed.
    int batch_put(int64_t decree,
                  const dsn::apps::update_request &update,
                  dsn::apps::update_response &resp);

    /// Add REMOVE op in batch.
    /// \returns 0 if success, non-0 if failure.
    /// NOTE that `resp` should not be moved or freed while the batch is not committed.
    int batch_remove(int64_t decree, const dsn::blob &key, dsn::apps::update_response &resp);

    /// Commit batch write.
    /// \returns 0 if success, non-0 if failure.
    /// NOTE that if the batch contains no updates, 0 is returned.
    int batch_commit(int64_t decree);

    /// Abort batch write.
    void batch_abort(int64_t decree);

    /// Write empty record.
    /// See this document (https://github.com/XiaoMi/pegasus/wiki/last_flushed_decree)
    /// to know why we must have empty write.
    int empty_put(int64_t decree);

private:
    friend class pegasus_write_service_test;

    class impl;
    std::unique_ptr<impl> _impl;

    uint64_t _batch_start_time;

    ::dsn::perf_counter_wrapper _pfc_put_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_put_qps;
    ::dsn::perf_counter_wrapper _pfc_remove_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_qps;

    ::dsn::perf_counter_wrapper _pfc_put_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_put_latency;
    ::dsn::perf_counter_wrapper _pfc_remove_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_latency;

    std::vector<::dsn::perf_counter *> _batch_qps_perfcounters;
    std::vector<::dsn::perf_counter *> _batch_latency_perfcounters;
};

} // namespace server
} // namespace pegasus
