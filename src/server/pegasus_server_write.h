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
    explicit pegasus_server_write(pegasus_server_impl *server);

    int on_batched_write_requests(dsn_message_t *requests,
                                  int count,
                                  int64_t decree,
                                  uint64_t timestamp);

private:
    void on_multi_put(multi_put_rpc &rpc)
    {
        _write_svc->multi_put(_put_ctx, rpc.request(), rpc.response());
    }

    void on_multi_remove(multi_remove_rpc &rpc)
    {
        _write_svc->multi_remove(_remove_ctx, rpc.request(), rpc.response());
    }

    void on_duplicate(duplicate_rpc &rpc)
    {
        on_duplicate_impl(false, rpc.request(), rpc.response());
    }

    void on_duplicate_impl(bool batched,
                           const dsn::apps::duplicate_request &request,
                           dsn::apps::duplicate_response &resp);

    /// Delay replying for the batched requests until all of them completes.
    int on_batched_writes(dsn_message_t *requests, int count, int64_t decree);

    void on_single_put_in_batch(put_rpc &rpc)
    {
        _write_svc->batch_put(_put_ctx, rpc.request(), rpc.response());
        request_key_check(_put_ctx.decree, rpc.dsn_request(), rpc.request().key);
    }

    void on_single_remove_in_batch(remove_rpc &rpc)
    {
        _write_svc->batch_remove(_remove_ctx, rpc.request(), rpc.response());
        request_key_check(_remove_ctx.decree, rpc.dsn_request(), rpc.request());
    }

    void on_single_duplicate_in_batch(duplicate_rpc &rpc)
    {
        on_duplicate_impl(true, rpc.request(), rpc.response());
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
    std::vector<duplicate_rpc> _batched_duplicate_rpc_batch;
    db_write_context _put_ctx;
    db_write_context _remove_ctx;

    bool _verbose_log;
    uint8_t _cluster_id;
};

} // namespace server
} // namespace pegasus