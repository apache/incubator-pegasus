/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <stdint.h>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>

#include "client/partition_resolver.h"
#include "common/serialization_helper/dsn.layer2_types.h"
#include "rpc/rpc_host_port.h"
#include "task/task.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
class message_ex;

namespace replication {

class partition_resolver_simple : public partition_resolver
{
public:
    partition_resolver_simple(host_port meta_server, const char *app_name);

    virtual ~partition_resolver_simple();

    virtual void resolve(uint64_t partition_hash,
                         std::function<void(resolve_result &&)> &&callback,
                         int timeout_ms) override;

    virtual void on_access_failure(int partition_index, error_code err) override;

    int get_partition_count() const { return _app_partition_count; }

private:
    struct partition_info
    {
        int timeout_count;
        ::dsn::partition_configuration pc;
    };
    mutable dsn::zrwlock_nr _config_lock;
    std::unordered_map<int, std::unique_ptr<partition_info>> _config_cache;

    int _app_id;
    int _app_partition_count;
    bool _app_is_stateful;

    typedef std::function<void(resolve_result &&)> callback_t;
    struct request_context : ref_counter
    {
        int partition_index;
        uint64_t partition_hash;
        callback_t callback;
        int timeout_ms;         // init timeout
        uint64_t timeout_ts_us; // timeout at this timing point

        zlock lock;             // [
        task_ptr timeout_timer; // when partition config is unknown at the first place
        bool completed;
        // ]
    };
    typedef ref_ptr<request_context> request_context_ptr;

    struct partition_context
    {
        task_ptr query_config_task;
        std::deque<request_context_ptr> requests;
    };

    typedef std::unordered_map<int, partition_context *> pending_replica_requests;

    mutable zlock _requests_lock;
    pending_replica_requests _pending_requests;
    std::deque<request_context_ptr> _pending_requests_before_partition_count_unknown;
    task_ptr _query_config_task;

    dsn::task_tracker _tracker;

private:
    // local routines
    host_port get_host_port(const partition_configuration &pc) const;
    error_code get_host_port(int partition_index, /*out*/ host_port &hp);
    void handle_pending_requests(std::deque<request_context_ptr> &reqs, error_code err);
    void clear_all_pending_requests();

    // with replica
    void call(request_context_ptr &&request, bool from_meta_ack = false);
    // void replica_rw_reply(error_code err, dsn::message_ex* request, dsn::message_ex* response,
    // request_context_ptr rc);
    void end_request(request_context_ptr &&request,
                     error_code err,
                     host_port addr,
                     bool called_by_timer = false) const;
    void on_timeout(request_context_ptr &&rc) const;

    // with meta server
    task_ptr query_config(int partition_index, int timeout_ms);
    void query_config_reply(error_code err,
                            dsn::message_ex *request,
                            dsn::message_ex *response,
                            int partition_index);
};
} // namespace replication
} // namespace dsn
