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

#include <boost/intrusive/slist.hpp>
#include <boost/intrusive/slist_hook.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
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
#include "utils/ports.h"

namespace boost::intrusive {
template <bool Enabled>
struct cache_last;
} // namespace boost::intrusive

namespace dsn {
class binary_reader;
class binary_writer;
class blob;
class gpid;
class task_spec;

namespace utils {
class latency_tracer;
} // namespace utils

namespace replication {

class mutation;

using mutation_ptr = dsn::ref_ptr<mutation>;

// As 2PC unit of PacificA, a mutation contains one or more write requests with header
// information related to PacificA algorithm in `data` member. It is appended to plog
// and written into prepare request broadcast to secondary replicas. It also holds the
// original client requests used to build the response to the client.
class mutation : public ref_counter, public boost::intrusive::slist_base_hook<>
{
public:
    mutation();
    ~mutation() override;

    DISALLOW_COPY_AND_ASSIGN(mutation);
    DISALLOW_MOVE_AND_ASSIGN(mutation);

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
    // - request: is from a client if non-null, otherwise is an empty write.
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
    // will not begin to be popped from the queue and processed until all of mutations before
    // it in the queue have been committed and applied into RocksDB. This field is only used
    // by primary replicas.
    //
    // For example, if the primary replica receives an incr request (with a base value of 1)
    // and the current configuration requires all atomic write requests to be idempotent, then:
    // 1. A mutation with `is_blocking` = true will be created to store this request and then
    // added to the mutation queue.
    // 2. This mutation request will only be dequeued after all previous write requests have
    // been applied.
    // 3. Next, the current base value 100 is read from the storage engine, and after performing
    // the incr operation, a single put request is created to store the final value 101.
    // 4. Another mutation is then created to store this idempotent single put request, which is
    // subsequently added to the write pipeline, including writing to the plog and broadcasting
    // to the secondary replicas.
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

// The mutation queue caches the mutations waiting to be processed in order by the write pipeline,
// including appended to plog and broadcast to secondary replicas. This class is only used by
// primary replicas.
//
// The entire queue is arranged in the order of `_blocking_mutation + _queue + _pending_mutation`,
// meaning that `_blocking_mutation` is the head of the queue if it is non-null, for the reason
// that it is enabled only when the mutation ready to get popped from the queue is a blocking
// mutation: it will block the entire queue from which none could get popped until all of the
// mutations before it have been applied.
//
// Once `_blocking_mutation` is cleared and becomes null, the head of the queue will be the first
// element of `_queue`. `_pending_mutation` is the tail of the queue, separated from `_queue` due
// to the following reasons:
// 1. As a carrier for storing client requests, each mutation needs to be size-limited. For each
// incoming request, we need to decide whether to continue storing it in the most recent mutation
// (i.e. `_pending_mutation`) or to create a new one.
// 2. The number of concurrent two-phase commits is limited. We should ensure the requests in
// each mutation could be processed as soon as possible if it does not reach the upper limit,
// even if the requests are in the latest mutation.
// 3. Some writes (such as non-single writes) do not allow batching. Once this kind of requests
// are received, a new mutation (`_pending_mutation`) should be created to hold them.
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

    DISALLOW_COPY_AND_ASSIGN(mutation_queue);
    DISALLOW_MOVE_AND_ASSIGN(mutation_queue);

    // Append the input request from the client to the queue by filling the latest mutation
    // with it.
    //
    // Parameters:
    // - request: must be non-null and from a client.
    //
    // Return the next mutation needing to be processed in order. Returning null means the
    // queue is being blocked or does not have any mutation.
    mutation_ptr add_work(dsn::message_ex *request);

    // Get the next mutation in order, typically called immediately after the current
    // mutation was applied, or the membership was changed and we became the primary
    // replica.
    //
    // Parameters:
    // - current_running_count: used to reset the current number of the mutations being
    // processed, typically the gap between the max committed decree and the max prepared
    // decree. `_current_op_count` is never decreased directly: this parameter provides
    // the only way to decrease it.
    //
    // Return the next mutation needing to be processed in order. Returning null means the
    // queue is being blocked or does not have any mutation.
    mutation_ptr next_work(int current_running_count);

    void acquire_row_lock(const mutation_ptr &mu);

    void release_row_lock(const mutation_ptr &mu);

    // Clear the entire queue.
    void clear();

    // Get the remaining unprocessed mutations and clear the entire queue.
    //
    // Parameters:
    // - queued_mutations: the output parameter used to hold the remaining unprocessed
    // mutations.
    void clear(std::vector<mutation_ptr> &queued_mutations);

private:
    // Promote `_pending_mutation` to `_queue`. Before the promotion, `_pending_mutation`
    // should not be null (otherwise the behaviour is undefined).
    void promote_pending();

    // If some conditions are met, promote `_pending_mutation` to `_queue`. Before the
    // promotion, `_pending_mutation` should not be null (otherwise the behaviour is
    // undefined).
    //
    // Parameters:
    // - spec: the specification for the incoming client request, used to check if this client
    // request is allowed to be batched.
    void try_promote_pending(task_spec *spec);

    bool row_locked(const mutation &mu);

    // Once the blocking mutation is enabled, the queue will be blocked and any mutation cannot
    // get popped. However, once the mutations before the blocking mutation have been applied
    // into RocksDB, the blocking mutation can be disabled and the queue will be "unblocked".
    // `_blocking_mutation` should not be null before this function is called.
    //
    // Return non-null blocking mutation if succeeding in unblocking, otherwise return null
    // which means the queue is still blocked.
    mutation_ptr try_unblock();

    // If immediately popped `mu` is not a blocking mutation, this function will do nothing
    // but increasing the count for the mutations being processed. Otherwise, it will set
    // `mu` to `_blocking_mutation` to enable the blocking mutation. `_blocking_mutation`
    // should be null before this function is called.
    //
    // Parameters:
    // - mu: the mutation immediately popped from the head of `_queue + _pending_mutation`.
    // Should not be null.
    //
    // Return the next mutation needing to be processed in order. Returning null means the
    // queue is being blocked or does not have any mutation.
    bool try_block(const mutation_ptr &mu);

    mutation_ptr try_block_queue();
    mutation_ptr try_block_pending();

    // Pop the mutation from the head of `_queue`.
    //
    // Return non-null mutation if the queue is not empty, otherwise return null.
    mutation_ptr pop_internal_queue()
    {
        if (_queue.empty()) {
            return {};
        }

        const auto work = _queue.front();
        _queue.pop();
        --(*_pcount);

        return work;
    }

    void reset_max_concurrent_ops(int max) { _max_concurrent_op = max; }

    replica *_replica;

    int _current_op_count;
    int _max_concurrent_op;
    bool _batch_write_disabled;

    volatile int *_pcount;
    mutation_ptr _pending_mutation;
    std::queue<mutation_ptr> _queue;

    // The tasks pushed into the mutation queue must first enter `_queue + _pending_mutation`.
    // When fetching tasks from `_queue + _pending_mutation` for execution: if the dequeued
    // mutation is blocking, and any of its write requests' hash keys are locked, it will be
    // pushed to the tail of `_blocking_mutations`.
    //
    // Therefore, when retrieving a task from the entire mutation queue, we first need to
    // sequentially check each mutation in `_blocking_mutations`:
    // - If the row lock is still held, no action is taken.
    // - If the row lock is released, the mutation is removed from `_blocking_mutations` and
    // selected as the next task from the mutation queue.
    //
    // This explains why `_blocking_mutations` is designed as a singly linked list — it allows
    // efficient element removal from the middle of the container with an O(1) time complexity.
    //
    // Only when no executable task is found in `_blocking_mutations`, the system proceeds to
    // check whether there are executable tasks in `_queue + _pending_mutation`.
    //
    // The reason for using `boost::intrusive::slist` as the singly linked list implementation
    // is that, when the template parameter `cache_last<true>` is set, it enables O(1) time
    // complexity for tail insertions.
    using blocking_mutation_list =
        boost::intrusive::slist<mutation, boost::intrusive::cache_last<true>>;
    blocking_mutation_list _blocking_mutations;

    // The structure of (key, value) is (partition_hash, count).
    //
    // Instead of directly using the actual hash key, we use the `partition_hash` generated on
    // the client side using the CRC64 algorithm as the key for row locks.
    //
    // Advantages of this approach:
    // 1. No computation required on the server side – The server can directly use the hash
    // value generated by the client. If we were to use the hash key as the row lock key, the
    // server would need to deserialize the entire client request using Thrift, which would
    // consume a significant number of CPU cycles.
    // 2. Fixed memory usage – The `partition_hash` is simply a 64-bit unsigned integer, which
    // takes up a fixed amount of memory and is often much smaller than the length of the
    // original hash key.
    //
    // Although, in theory, different hash keys might generate colliding `partition_hash` values,
    // the probability of this happening is low. Even if a collision does occur occasionally,
    // it does not affect the correctness of program execution.
    //
    // The value stored in the row lock structure represents the number of hash keys associated
    // with a `partition_hash` that are currently in the 2PC phase but have not yet been applied
    // to the storage engine:
    // - When a write request enters the 2PC phase, the value increases by 1 (if the corresponding
    // `partition_hash` does not exist in the map, it is inserted). This indicates that the hash
    // key has been locked.
    // - If another atomic write request with the same hash key arrives later, it will be rejected
    // from entering the 2PC phase because the partition_hash has already been locked. The
    // rejection remains in effect until the value count reaches 0, meaning the partition_hash has
    // been removed from the map.
    // - Once a write request is applied to the storage engine, its corresponding count value
    // decreases by 1. If the count reaches 0, the partition_hash is removed from the map.
    //
    // TODO(wangdan): consider comparing performance between boost::unordered_flat_map
    // and absl::flat_hash_map, both of which are based on open addressing.
    // Introducing absl::flat_hash_map is very easy, just by:
    // using row_lock_map = absl::flat_hash_map<uint64_t, size_t>;
    // I've tried to introduce absl::flat_hash_map; however, it could not pass the ASAN
    // tests due to segmentation fault caused by dereferencing a null pointer inside
    // "absl/container/internal/raw_hash_set.h". I'll try it again later.
    using row_lock_map = boost::unordered_flat_map<uint64_t, size_t>;
    row_lock_map _row_locks;
};

} // namespace replication
} // namespace dsn
