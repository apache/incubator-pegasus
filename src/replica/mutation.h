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
#include <atomic>
#include <functional>
#include <memory>
#include <queue>
#include <vector>

#include "common/replication_common.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "rpc/rpc_message.h"
#include "runtime/api_layer1.h"
#include "task/task.h"
#include "utils/autoref_ptr.h"
#include "utils/fmt_logging.h"

namespace dsn {
class binary_reader;
class binary_writer;
class blob;
class gpid;

namespace utils {
class latency_tracer;
} // namespace utils

namespace replication {

class mutation;

using mutation_ptr = dsn::ref_ptr<mutation>;

// Mutation is 2PC unit of PacificA, which wraps one or more client requests and adds header
// informations related to PacificA algorithm for them. Both header and client request content
// are put into "data" member.
class mutation : public ref_counter
{
public:
    mutation();
    ~mutation() override;

    mutation(const mutation &) = delete;
    mutation &operator=(const mutation &) = delete;
    mutation(mutation &&) = delete;
    mutation &operator=(mutation &&) = delete;

    // copy mutation from an existing mutation, typically used in partition split
    // mutation should not reply to client, because parent has already replied
    static mutation_ptr copy_no_reply(const mutation_ptr &old_mu);

    // state inquery
    const char *name() const { return _name; }
    const uint64_t tid() const { return _tid; }
    bool is_logged() const { return _not_logged == 0; }
    bool is_ready_for_commit() const { return _private0 == 0; }
    const std::vector<dsn::message_ex *> &prepare_requests() const { return _prepare_requests; }
    void add_prepare_request(dsn::message_ex *request)
    {
        if (nullptr != request) {
            _prepare_requests.push_back(request);
            request->add_ref(); // released on dctor
        }
    }
    unsigned int left_secondary_ack_count() const { return _left_secondary_ack_count; }
    unsigned int left_potential_secondary_ack_count() const
    {
        return _left_potential_secondary_ack_count;
    }
    bool is_child_acked() const { return !_wait_child; }
    bool is_error_acked() const { return _is_error_acked; }
    ::dsn::task_ptr &log_task() { return _log_task; }
    node_tasks &remote_tasks() { return _prepare_or_commit_tasks; }
    bool is_prepare_close_to_timeout(int gap_ms, int timeout_ms)
    {
        return dsn_now_ms() + gap_ms >= _prepare_ts_ms + timeout_ms;
    }
    uint64_t create_ts_ns() const { return _create_ts_ns; }
    ballot get_ballot() const { return data.header.ballot; }
    decree get_decree() const { return data.header.decree; }

    // state change
    void set_id(ballot b, decree c);
    void set_timestamp(int64_t timestamp) { data.header.timestamp = timestamp; }

    // Append a write request to this mutation, and also hold it if it is from a client
    // to build the response to the client later.
    //
    // Parameters:
    // - request: it is from a client if non-null, otherwise it is an empty write.
    void add_client_request(dsn::message_ex *request);

    void copy_from(mutation_ptr &old);
    void set_logged()
    {
        CHECK(!is_logged(), "");
        _not_logged = 0;
    }
    unsigned int decrease_left_secondary_ack_count() { return --_left_secondary_ack_count; }
    unsigned int decrease_left_potential_secondary_ack_count()
    {
        return --_left_potential_secondary_ack_count;
    }
    void set_left_secondary_ack_count(unsigned int count) { _left_secondary_ack_count = count; }
    void set_left_potential_secondary_ack_count(unsigned int count)
    {
        _left_potential_secondary_ack_count = count;
    }
    void wait_child() { _wait_child = true; }
    void child_acked() { _wait_child = false; }
    void set_error_acked() { _is_error_acked = true; }
    int clear_prepare_or_commit_tasks();
    void wait_log_task() const;
    uint64_t prepare_ts_ms() const { return _prepare_ts_ms; }
    void set_prepare_ts() { _prepare_ts_ms = dsn_now_ms(); }

    // >= 1 MB
    bool is_full() const { return _appro_data_bytes >= 1024 * 1024; }
    int appro_data_bytes() const { return _appro_data_bytes; }

    // read & write mutation data
    //
    // "mutation_update.code" should be marshalled as string for cross-process compatiblity,
    // because:
    //   - the private log may be transfered to other node with different program
    //   - the private/shared log may be replayed by different program when server restart
    void write_to(const std::function<void(const blob &)> &inserter) const;
    void write_to(binary_writer &writer, dsn::message_ex *to) const;
    static mutation_ptr read_from(binary_reader &reader, dsn::message_ex *from);

    static void write_mutation_header(binary_writer &writer, const mutation_header &header);
    static void read_mutation_header(binary_reader &reader, mutation_header &header);

    // data
    mutation_data data;

    // user requests
    std::vector<dsn::message_ex *> client_requests;

    // A mutation will be a blocking mutation if `is_blocking` is true. A blocking mutation
    // will not begin to be poped from the queue and processed until all of mutations before
    // it in the queue have been committed and applied into RocksDB. This field is only used
    // by primary replicas.
    bool is_blocking{false};

    // The original request received from the client. While making an atomic request (incr,
    // check_and_set and check_and_mutate) idempotent, an extra variable is needed to hold
    // its original request for the purpose of replying to the client.
    dsn::message_ptr original_request;

    std::shared_ptr<dsn::utils::latency_tracer> _tracer;

    void set_is_sync_to_child(bool sync_to_child) { _is_sync_to_child = sync_to_child; }
    bool is_sync_to_child() { return _is_sync_to_child; }

private:
    union
    {
        struct
        {
            unsigned int _not_logged : 1;
            unsigned int _left_secondary_ack_count : 15;
            unsigned int _left_potential_secondary_ack_count : 14;
            // Used for partition split
            // _wait_child = true : child prepare mutation synchronously, its parent should wait for
            // child ack
            bool _wait_child : 1;
            // Used for partition split
            // when prepare failed when child prepare mutation synchronously, secondary may try to
            // ack to primary twice, we use _is_error_acked to restrict only ack once
            bool _is_error_acked : 1;
        };
        uint32_t _private0;
    };

    uint64_t _prepare_ts_ms;
    ::dsn::task_ptr _log_task;
    node_tasks _prepare_or_commit_tasks;
    std::vector<dsn::message_ex *> _prepare_requests; // may combine duplicate requests
    char _name[60];                                   // app_id.partition_index.ballot.decree
    int _appro_data_bytes;
    uint64_t _create_ts_ns; // for profiling
    uint64_t _tid;          // trace id, unique in process
    static std::atomic<uint64_t> s_tid;
    bool _is_sync_to_child; // for partition split
};

class replica;

// mutation queue are queues for mutations waiting to send.
// more precisely: for client requests waiting to send.
// mutations are queued as "_queue + _pending_mutation". that is to say, _queue.first is the first
// element in the queue, and pending_mutations is the last.
//
// However, once _blocking_mutation is non-null, it is the first element.
//
// we keep 2 structure "hdr" and "pending_mutation" coz:
// 1. as a container of client requests, capacity of a mutation is limited, so incoming client
//    requets should be packed into different mutations
// 2. number of preparing mutations is also limited, so we should queue new created mutations and
//    try to send them as soon as the concurrent condition satisfies.
class mutation_queue
{
public:
    mutation_queue(replica *r, gpid gpid, int max_concurrent_op, bool batch_write_disabled);

    ~mutation_queue()
    {
        clear();
        CHECK(_queue.empty(),
              "work queue is deleted when there are still {} running ops or pending work items "
              "in queue",
              _current_op_count);
    }

    mutation_queue(const mutation_queue &) = delete;
    mutation_queue &operator=(const mutation_queue &) = delete;
    mutation_queue(mutation_queue &&) = delete;
    mutation_queue &operator=(mutation_queue &&) = delete;

    mutation_ptr add_work(dsn::message_ex *request);

    void clear();
    // called when you want to clear the mutation_queue and want to get the remaining messages
    void clear(std::vector<mutation_ptr> &queued_mutations);

    // called when the curren operation is completed or replica configuration is change,
    // which triggers further round of operations as returned
    mutation_ptr check_possible_work(int current_running_count);

private:
    mutation_ptr try_unblock();
    mutation_ptr try_block(mutation_ptr &mu);

    mutation_ptr unlink_next_workload()
    {
        if (_queue.empty()) {
            return {};
        }

        const auto work = _queue.front();
        _queue.pop();
        --(*_pcount);

        return work;
    }

    void reset_max_concurrent_ops(int max_c) { _max_concurrent_op = max_c; }

    replica *_replica;

    int _current_op_count;
    int _max_concurrent_op;
    bool _batch_write_disabled;

    volatile int *_pcount;
    mutation_ptr _pending_mutation;
    std::queue<mutation_ptr> _queue;

    // Once a mutation that would get popped is blocking, it should firstly be put in
    // `_blocking_mutation`; then, the queue would always return nullptr until previous
    // mutations have been committed and applied into the rocksdb of primary replica.
    mutation_ptr _blocking_mutation;
};

} // namespace replication
} // namespace dsn
