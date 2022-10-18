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

#include "replica/replica_base.h"

#include "base/pegasus_rpc_types.h"
#include "pegasus_write_service.h"

namespace pegasus {
namespace server {

/// This class implements the interface of `pegasus_sever_impl::on_batched_write_requests`.
class pegasus_server_write : public dsn::replication::replica_base
{
public:
    pegasus_server_write(pegasus_server_impl *server, bool verbose_log);

    /// \return error code returned by rocksdb, i.e rocksdb::Status::code.
    /// **NOTE**
    /// Error returned is regarded as the failure of replica, thus will trigger
    /// cluster membership changes. Make sure no error is returned because of
    /// invalid user argument.
    /// As long as the returned error is 0, the operation is guaranteed to be
    /// successfully applied into rocksdb, which means an empty_put will be called
    /// even if there's no write.
    int on_batched_write_requests(dsn::message_ex **requests,
                                  int count,
                                  int64_t decree,
                                  uint64_t timestamp);

    void set_default_ttl(uint32_t ttl);

private:
    /// Delay replying for the batched requests until all of them complete.
    int on_batched_writes(dsn::message_ex **requests, int count);

    int on_single_put_in_batch(put_rpc &rpc)
    {
        int err = _write_svc->batch_put(_write_ctx, rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request().key);
        return err;
    }

    int on_single_remove_in_batch(remove_rpc &rpc)
    {
        int err = _write_svc->batch_remove(_decree, rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request());
        return err;
    }

    // Ensure that the write request is directed to the right partition.
    // In verbose mode it will log for every request.
    void request_key_check(int64_t decree, dsn::message_ex *m, const dsn::blob &key);

private:
    void init_non_batch_write_handlers();

    friend class pegasus_server_write_test;
    friend class pegasus_write_service_test;
    friend class pegasus_write_service_impl_test;
    friend class rocksdb_wrapper_test;

    std::unique_ptr<pegasus_write_service> _write_svc;
    std::vector<put_rpc> _put_rpc_batch;
    std::vector<remove_rpc> _remove_rpc_batch;

    db_write_context _write_ctx;
    int64_t _decree;

    const bool _verbose_log;

    typedef std::map<dsn::task_code, std::function<int(dsn::message_ex *)>> non_batch_writes_map;
    non_batch_writes_map _non_batch_write_handlers;

    ::dsn::perf_counter_wrapper _pfc_recent_corrupt_write_count;
};

} // namespace server
} // namespace pegasus
