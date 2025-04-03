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

    // A mutation will be a blocking candidate if this field is true. Typically, to hold an
    // atomic write request while idempotence is enabled, a mutation will be will be created
    // as a blocking candidate.
    //
    // A blocking candidate will be blocked once it has a locked hash key: it will not get
    // popped from mutation queue until it does not has any locked hash key.
    //
    // For example, the primary replica receives an incr request from a client. If the current
    // configuration requires all atomic write requests to be idempotent, then:
    // 1. A mutation will be created as a blocking candidate to hold this request and then
    // appended to the mutation queue.
    // 2. Once its hash key is locked, it will be blocked and cannot get popped.
    // 3. It can get popped only after its hash key becomes unlocked.
    // 4. After popped, the current base value 100 is read from the storage engine, and after
    // performing incr operation, a single put request is created to store the final value 101.
    // 5. Another mutation is then created to hold this idempotent single put request.
    // 6. Subsequently the new mutation enters 2PC phase, appended to plog and broadcast to
    // secondary replicas.
    //
    // This field is only used by primary replicas.
    bool is_blocking_candidate{false};

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

// When the primary replica receives a write request from the client, it is assembled into a
// mutation (batched if allowed) and pushed to the tail of the mutation queue for caching.
// Meanwhile, mutations popped from the mutation queue will gradually enter the 2PC phase.
//
// The entire queue is ordered as `_blocking_mutations + _queue + _pending_mutation`. The reason
// why `_blocking_mutations` is at the head of this queue is that it caches mutations that need
// to be blocked (those blocking candidates whose hash keys are locked). When deciding which
// mutation could be dequeued from the mutation queue and returned to be processed in 2PC phase,
// firstly traverse `_blocking_mutations`: if any mutation is found unlocked, dequeue and return
// it immediately. If no mutation could be dequeued, then turn to `_queue + _pending_mutation`:
// - Start from the head. If a mutation is a blocking candidate and contains a write request whose
//   hash key is locked, it should be blocked: just append it to the tail of `_blocking_mutations`
//   and continue checking the next mutation. Otherwise, it will be dequeued and returned.
// - If none of mutations could be dequeued, just return null.
//
// As a carrier for storing client requests, each mutation needs to be size-limited. For each
// incoming request, we need to decide whether to continue storing it in the most recent mutation
// or to create a new one to hold it. That's why `_pending_mutation` is separated from `_queue`:
// `_pending_mutation` is the most recent mutation, i.e. the tail of the mutation queue. Any
// client request that is appended to the mutation queue will firstly added into it.
//
// Current `_pending_mutation` will be promoted (i.e. appended) to `_queue` and reset to a new
// mutation to hold the subsequent client requests, when:
// 1. Current `_pending_mutation` is big enough.
// 2. The received write request does not allow batching (such as non-single writes).
// 3. FLAGS_batch_write_disabled is set to true. As the global configuration, it decides whether
// to disallow batching for all kinds of write requests.
//
// This class is only used by primary replicas.
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

    // Fill the latest mutation with the incoming client request, then append it to the queue.
    // And finally, get the next mutation from the queue.
    //
    // Parameters:
    // - request: the incoming write request from a client, must be non-null.
    //
    // Return the next mutation needing to be processed in 2PC phase. If the returned mutation
    // is null, it means the queue is empty, or all mutations in the queue is being blocked.
    mutation_ptr add_work(dsn::message_ex *request);

    // Get the next mutation from the queue, typically called immediately after the current
    // mutation was applied, or the membership was changed and we became the primary replica.
    //
    // Parameters:
    // - current_running_count: used to reset the current number of the mutations being processed
    // concurrently in 2PC phase. It is typically the gap between the max committed decree and
    // the max prepared decree. `_current_op_count` is never decreased directly: this parameter
    // provides the only way to decrease it.
    //
    // Return the next mutation needing to be processed in 2PC phase. If the returned mutation
    // is null, it means the queue is empty, or all mutations in the queue is being blocked.
    mutation_ptr next_work(int current_running_count);

    // Acquire row locks for each hash key in the mutation `mu`.
    //
    // The reason we do not acquire the lock immediately upon dequeuing is that between dequeuing
    // and actually entering the 2PC phase, there are some processing steps which are sequential
    // operations executed in the same unique thread dedicated to this hash key and the dequeue
    // operation. These steps may fail and return an error to the client. Therefore, call this
    // function to acquire locks only when the mutation is truly about to enter the 2PC phase.
    void acquire_row_lock(const mutation_ptr &mu);

    // Release row locks for each hash key in the mutation `mu`.
    //
    // Once a mutation is applied to the storage engine, regardless of success or failure (if
    // the application fails and we are the primary replica, we will later request the meta
    // server to remove it from the membership), we must call this interface to release the row
    // locks corresponding to the mutation. This ensures that blocked mutations can be dequeued,
    // preventing a scenario where they remain blocked indefinitely due to unreleased row locks.
    // void release_row_lock(const mutation_ptr &mu);

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

    // Check each client request within the mutation `mu` one by one:
    // - If any request's `partition_hash` exists in the row lock mapping table, it means that
    // the corresponding hash key is locked. In this case, return true, indicating that the
    // entire mutation is locked.
    // - Otherwise, if none of the requests are locked, return false, meaning that the mutation
    // is not locked.
    bool row_locked(const mutation &mu);
    bool applied(decree d) const;

    // Sequentially check the mutations in `_blocking_mutations` from head to tail:
    // - If a mutation is still locked, which means it still needs to be blocked, so continue
    // checking the next mutation.
    // - Otherwise, this mutation no longer needs to be blocked - it will be removed from
    // `_blocking_mutations` and returned, to proceed to the 2PC phase at any time.
    //
    // If all mutations in `_blocking_mutations` are still locked, return null, which means
    // none of the mutations in `_blocking_mutations` could be "unblocked".
    mutation_ptr try_unblock();

    // `mu` is a mutation dequeued from `_queue + _pending_mutation` and cannot be null. If
    // it is a blocking candidate and is currently locked, it should be pushed to the tail
    // of `_blocking_mutations` and return true. Otherwise, it does not need to be blocked,
    // return false indicating that it could proceed to the 2PC phase at any time.
    bool try_block(const mutation_ptr &mu);

    // Start from the head of `_queue`: if the popped mutation is blocked, push it to the
    // tail of `_blocking_mutations`; otherwise, return it to be processed in 2PC phase.
    // If all of the mutations in `_queue` are blocked, return null.
    mutation_ptr pop_or_block_queue();

    // If `_pending_mutation` is blocked, push it to the tail of `_blocking_mutations` and
    // return null; otherwise, return it to be processed in 2PC phase.
    mutation_ptr pop_or_block_pending();

    // Pop the mutation from the head of `_queue`. Return non-null mutation if the queue is
    // not empty, otherwise return null.
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

    // The current count of the mutations being processed concurrently in 2PC phase.
    int _current_op_count;

    // The max allowed count of the mutations being processed concurrently in 2PC phase.
    // Currently this is set to FLAGS_staleness_for_commit.
    int _max_concurrent_op;

    // Whether the write requests are allowed to be batched. Currently this is set to
    // FLAGS_batch_write_disabled.
    bool _batch_write_disabled;

    volatile int *_pcount;
    mutation_ptr _pending_mutation;
    std::queue<mutation_ptr> _queue;

    // The tasks pushed into the mutation queue must first enter `_queue + _pending_mutation`.
    // When fetching tasks from `_queue + _pending_mutation` for execution: if the dequeued
    // mutation is a blocking candidate, and any of its write requests' hash keys are locked,
    // it will be pushed to the tail of `_blocking_mutations`.
    //
    // Therefore, when retrieving a task from the entire mutation queue, we first need to
    // sequentially check each mutation in `_blocking_mutations`:
    // - If the row lock is still held, no action is taken.
    // - If the row lock is released, the mutation is removed from `_blocking_mutations` and
    // selected as the next task from the mutation queue.
    //
    // This explains why `_blocking_mutations` is designed as a singly linked list — it allows
    // efficient element removal from the middle of the container with an O(1) time complexity.
    // The reason for not using a doubly linked list is that we only need to traverse in a
    // single direction from head to tail. Using a singly linked list is more memory-efficient.
    //
    // Only when no executable task is found in `_blocking_mutations`, the system proceeds to
    // check whether there are executable tasks in `_queue + _pending_mutation`.
    //
    // The reasons for using boost::intrusive::slist as the singly linked list implementation
    // are:
    // 1. Low memory overhead due to intrusive design – it eliminates the need for extra node
    // memory allocation on the heap for each element in the container. Each element object
    // only requires an additional next pointer, which is the only extra memory overhead.
    // 2. O(1) time complexity for tail insertions – when the template parameter is set to
    // cache_last<true>, it allows constant-time (O(1)) insertion at the tail.
    using blocking_mutation_list =
        boost::intrusive::slist<mutation, boost::intrusive::cache_last<true>>;
    blocking_mutation_list _blocking_mutations;

    // The row lock mapping table. The structure of (key, value) is (partition_hash, count).
    // Instead of directly using the actual hash key, we use the `partition_hash` generated on
    // the client side using the CRC64 algorithm as the key for row locks.
    //
    // Advantages of this approach:
    // 1. No computation required on the server side – the server can directly use the hash
    // value generated by the client. If we were to use the hash key as the row lock key, the
    // server would need to deserialize the entire client request using Thrift, which would
    // consume a significant number of CPU cycles.
    // 2. Fixed memory usage – the `partition_hash` is simply a 64-bit unsigned integer, which
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
    //
    // Introducing absl::flat_hash_map is very easy, just by:
    //     using row_lock_map = absl::flat_hash_map<uint64_t, size_t>;
    //
    // I've tried to introduce absl::flat_hash_map; however, it could not pass the ASAN
    // tests due to segmentation fault caused by dereferencing a null pointer inside
    // "absl/container/internal/raw_hash_set.h". I'll try it again later.
    using lru_row_list = std::list<std::pair<uint64_t, decree>>;
    using row_lock_map = boost::unordered_flat_map<uint64_t, lru_row_list::iterator>;
    row_lock_map _row_locks;
    lru_row_list _lru_rows;
};

} // namespace replication
} // namespace dsn
