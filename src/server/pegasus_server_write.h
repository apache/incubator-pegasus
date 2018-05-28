// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/replica_base.h>

#include "base/pegasus_rpc_types.h"
#include "pegasus_write_service.h"

namespace pegasus {
namespace server {

/// This class implements the interface of `pegasus_sever_impl::on_batched_write_requests`.
class pegasus_server_write : public dsn::replication::replica_base
{
public:
    pegasus_server_write(pegasus_server_impl *server, bool verbose_log);

    int on_batched_write_requests(dsn_message_t *requests,
                                  int count,
                                  int64_t decree,
                                  uint64_t timestamp);

private:
    void on_multi_put(multi_put_rpc &rpc)
    {
        _write_svc->multi_put(_decree, rpc.request(), rpc.response());
    }

    void on_multi_remove(multi_remove_rpc &rpc)
    {
        _write_svc->multi_remove(_decree, rpc.request(), rpc.response());
    }

    /// Delay replying for the batched requests until all of them complete.
    int on_batched_writes(dsn_message_t *requests, int count, int64_t decree);

    void on_single_put_in_batch(put_rpc &rpc)
    {
        _write_svc->batch_put(rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request().key);
    }

    void on_single_remove_in_batch(remove_rpc &rpc)
    {
        _write_svc->batch_remove(rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request());
    }

    // Ensure that the write request is directed to the right partition.
    // In verbose mode it will log for every request.
    void request_key_check(int64_t decree, dsn_message_t m, const dsn::blob &key);

private:
    friend class pegasus_server_write_test;
    friend class pegasus_write_service_test;

    std::unique_ptr<pegasus_write_service> _write_svc;
    std::vector<put_rpc> _put_rpc_batch;
    std::vector<remove_rpc> _remove_rpc_batch;

    int64_t _decree;

    const bool _verbose_log;
};

} // namespace server
} // namespace pegasus
