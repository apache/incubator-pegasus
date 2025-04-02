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

#include "mutation.h"

#include <boost/intrusive/detail/slist_iterator.hpp>
#include <boost/unordered/detail/foa/table.hpp>
#include <algorithm>
#include <cinttypes>
#include <cstring>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "common/gpid.h"
#include "common/replication.codes.h"
#include "replica.h"
#include "runtime/api_task.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"

DSN_DEFINE_uint64(
    replication,
    abnormal_write_trace_latency_threshold,
    1000UL * 1000UL * 1000UL, // 1s
    "Latency trace will be logged when exceed the write latency threshold, in nanoseconds");
DSN_TAG_VARIABLE(abnormal_write_trace_latency_threshold, FT_MUTABLE);

namespace dsn::replication {

std::atomic<uint64_t> mutation::s_tid(0);

mutation::mutation()
    : _tracer(std::make_shared<dsn::utils::latency_tracer>(
          false, "mutation", FLAGS_abnormal_write_trace_latency_threshold)),
      _private0(0),
      _prepare_ts_ms(0),
      _name{0},
      _appro_data_bytes(sizeof(mutation_header)),
      _create_ts_ns(dsn_now_ns()),
      _tid(++s_tid),
      _is_sync_to_child(false)
{
    _not_logged = 1;
    _left_secondary_ack_count = 0;
    _left_potential_secondary_ack_count = 0;
    _wait_child = false;
    _is_error_acked = false;
    strcpy(_name, "0.0.0.0");
}

mutation_ptr mutation::copy_no_reply(const mutation_ptr &old_mu)
{
    mutation_ptr mu(new mutation());
    mu->_private0 = old_mu->_private0;
    strcpy(mu->_name, old_mu->_name);
    mu->_appro_data_bytes = old_mu->_appro_data_bytes;
    mu->data = old_mu->data;
    mu->_is_sync_to_child = old_mu->is_sync_to_child();
    // create a new message without client information, it will not rely
    for (auto req : old_mu->client_requests) {
        if (req != nullptr) {
            dsn::message_ex *new_req = message_ex::copy_message_no_reply(*req);
            mu->client_requests.emplace_back(new_req);
        } else {
            mu->client_requests.emplace_back(req);
        }
    }
    return mu;
}

mutation::~mutation()
{
    for (auto &r : client_requests) {
        if (r != nullptr) {
            r->release_ref();
        }
    }

    for (auto &request : _prepare_requests) {
        request->release_ref();
    }
}

void mutation::set_id(ballot b, decree c)
{
    data.header.ballot = b;
    data.header.decree = c;

    snprintf_p(_name,
               sizeof(_name),
               "%" PRId32 ".%" PRId32 ".%" PRId64 ".%" PRId64,
               data.header.pid.get_app_id(),
               data.header.pid.get_partition_index(),
               data.header.ballot,
               data.header.decree);
}

void mutation::copy_from(mutation_ptr &old)
{
    data.updates = old->data.updates;
    client_requests = old->client_requests;
    _appro_data_bytes = old->_appro_data_bytes;
    _create_ts_ns = old->_create_ts_ns;

    for (auto &r : client_requests) {
        if (r != nullptr) {
            // release in dctor
            r->add_ref();
        }
    }

    // let's always re-append the mutation to
    // replication logs as the ballot number
    // is changed, to ensure the invariance:
    // if decree(A) >= decree(B)
    // then ballot(A) >= ballot(B)
    /*if (old->is_logged())
    {
        set_logged();
        data.header.log_offset = old->data.header.log_offset;
    }
    */

    _prepare_requests = old->prepare_requests();
    for (auto &request : _prepare_requests) {
        request->add_ref();
    }
}

void mutation::add_client_request(dsn::message_ex *request)
{
    data.updates.emplace_back();
    mutation_update &update = data.updates.back();
    _appro_data_bytes += 32; // approximate code size

    if (request != nullptr) {
        update.code = request->rpc_code();
        update.serialization_type =
            static_cast<dsn_msg_serialize_format>(request->header->context.u.serialize_format);
        update.__set_start_time_ns(static_cast<int64_t>(dsn_now_ns()));
        request->add_ref(); // released on dctor

        void *ptr = nullptr;
        size_t size = 0;
        CHECK(request->read_next(&ptr, &size), "payload is not present");
        request->read_commit(0); // so we can re-read the request buffer in replicated app
        update.data.assign(static_cast<const char *>(ptr), 0, size);

        _appro_data_bytes += static_cast<int>(sizeof(int) + size); // data size
    } else {
        update.code = RPC_REPLICATION_WRITE_EMPTY;
        _appro_data_bytes += static_cast<int>(sizeof(int)); // empty data size
    }

    client_requests.push_back(request);

    CHECK_EQ(client_requests.size(), data.updates.size());
}

void mutation::write_to(const std::function<void(const blob &)> &inserter) const
{
    binary_writer writer(1024);
    write_mutation_header(writer, data.header);
    writer.write_pod(static_cast<int>(data.updates.size()));
    for (const mutation_update &update : data.updates) {
        // write task_code as string to make it cross-process compatible.
        // avoid memory copy, equal to writer.write(std::string)
        const char *cstr = update.code.to_string();
        int len = static_cast<int>(strlen(cstr));
        writer.write_pod(len);
        if (len > 0)
            writer.write(cstr, len);

        writer.write_pod(static_cast<int>(update.serialization_type));

        writer.write_pod(static_cast<int>(update.data.length()));
    }
    inserter(writer.get_buffer());
    for (const mutation_update &update : data.updates) {
        inserter(update.data);
    }
}

void mutation::write_to(binary_writer &writer, dsn::message_ex * /*to*/) const
{
    write_mutation_header(writer, data.header);
    writer.write_pod(static_cast<int>(data.updates.size()));
    for (const mutation_update &update : data.updates) {
        // write task_code as string to make it cross-process compatible.
        // avoid memory copy, equal to writer.write(std::string)
        const char *cstr = update.code.to_string();
        int len = static_cast<int>(strlen(cstr));
        writer.write_pod(len);
        if (len > 0)
            writer.write(cstr, len);

        writer.write_pod(static_cast<int>(update.serialization_type));

        writer.write_pod(static_cast<int>(update.data.length()));
    }
    // TODO(qinzuoyan): directly append buffer to message to avoid memory copy
    for (const mutation_update &update : data.updates) {
        writer.write(update.data.data(), update.data.length());
    }
}

/*static*/ mutation_ptr mutation::read_from(binary_reader &reader, dsn::message_ex *from)
{
    mutation_ptr mu(new mutation());
    read_mutation_header(reader, mu->data.header);

    int size = 0;
    reader.read_pod(size);
    mu->data.updates.resize(size);
    std::vector<int> lengths(size, 0);
    for (int i = 0; i < size; ++i) {
        std::string name;
        reader.read(name);
        ::dsn::task_code code = dsn::task_code::try_get(name, TASK_CODE_INVALID);
        CHECK_NE_MSG(code, TASK_CODE_INVALID, "invalid mutation task code: {}", name);
        mu->data.updates[i].code = code;

        int type = 0;
        reader.read_pod(type);
        mu->data.updates[i].serialization_type = type;

        reader.read_pod(lengths[i]);
    }
    for (int i = 0; i < size; ++i) {
        reader.read(mu->data.updates[i].data, lengths[i]);
    }

    mu->client_requests.resize(mu->data.updates.size());
    mu->add_prepare_request(from);

    snprintf_p(mu->_name,
               sizeof(mu->_name),
               "%" PRId32 ".%" PRId32 ".%" PRId64 ".%" PRId64,
               mu->data.header.pid.get_app_id(),
               mu->data.header.pid.get_partition_index(),
               mu->data.header.ballot,
               mu->data.header.decree);

    return mu;
}

/*static*/ void mutation::write_mutation_header(binary_writer &writer,
                                                const mutation_header &header)
{
    writer.write_pod((int64_t)0);
    writer.write_pod(header.pid.value());
    writer.write_pod(header.ballot);
    writer.write_pod(header.decree);
    writer.write_pod(header.log_offset);
    writer.write_pod(header.last_committed_decree);
    writer.write_pod(header.timestamp);
}

/*static*/ void mutation::read_mutation_header(binary_reader &reader, mutation_header &header)
{
    // original code:
    //   reader.read_pod(mu->data.header);
    // this will read 7*8=56 bytes of:
    //   - vptr (which must > 64)
    //   - gpid
    //   - ballot
    //   - decree
    //   - log_offset
    //   - last_committed_decree
    //   - __isset
    //
    // new code (also 7*8=56 bytes):
    //   - version
    //   - gpid
    //   - decree
    //   - ballot
    //   - log_offset
    //   - last_committed_decree
    //   - timestamp
    int64_t version = 0;
    reader.read_pod(version);
    uint64_t pid_value = 0;
    reader.read_pod(pid_value);
    header.pid.set_value(pid_value);
    reader.read_pod(header.ballot);
    reader.read_pod(header.decree);
    reader.read_pod(header.log_offset);
    reader.read_pod(header.last_committed_decree);
    if (version == 0) {
        reader.read_pod(header.timestamp);
    } else if (version > 64) {
        // version is vptr, we need read '__isset', and ignore it
        int64_t isset;
        reader.read_pod(isset);
        header.timestamp = 0;
    } else {
        CHECK(false, "invalid mutation log version: {:#018x}", version);
    }
}

int mutation::clear_prepare_or_commit_tasks()
{
    int c = 0;
    for (auto it = _prepare_or_commit_tasks.begin(); it != _prepare_or_commit_tasks.end(); ++it) {
        if (it->second->cancel(true)) {
            c++;
        }
    }

    _prepare_or_commit_tasks.clear();
    return c;
}

void mutation::wait_log_task() const
{
    if (_log_task != nullptr) {
        _log_task->wait();
    }
}

mutation_queue::mutation_queue(replica *r,
                               gpid gpid,
                               int max_concurrent_op,
                               bool batch_write_disabled)
    : _replica(r),
      _current_op_count(0),
      _max_concurrent_op(max_concurrent_op),
      _batch_write_disabled(batch_write_disabled)
{
    CHECK_NE_MSG(gpid.get_app_id(), 0, "invalid gpid");
    _pcount = dsn_task_queue_virtual_length_ptr(RPC_PREPARE, gpid.thread_hash());
}

void mutation_queue::promote_pending()
{
    _queue.push(_pending_mutation);
    _pending_mutation.reset();
    ++(*_pcount);
}

void mutation_queue::try_promote_pending(task_spec *spec)
{
    // Promote `_pending_mutation` to `_queue` in following cases:
    // - this client request (whose specification is `spec`) is not allowed to be batched, or
    // - the size of `_pending_mutation` reaches the upper limit, or
    // - batch write is disabled (initialized by FLAGS_batch_write_disabled).
    //
    // Choose `_batch_write_disabled` as the last condition to be checked to optimize the
    // performance by short-circuit evaluation since it is actually FLAGS_batch_write_disabled
    // which is mostly set false by default while other conditions vary with different incoming
    // client requests.
    if (spec->rpc_request_is_write_allow_batch && !_pending_mutation->is_full() &&
        !_batch_write_disabled) {
        return;
    }

    promote_pending();
}

#define CHECK_RPC_REQUEST_IS_WRITE(request)                                                        \
    do {                                                                                           \
        const auto *__spec = task_spec::get(request->rpc_code());                                  \
        CHECK_NOTNULL(__spec, "RPC code {} not found", request->rpc_code());                       \
        CHECK_TRUE(__spec->rpc_request_is_write_operation);                                        \
    } while (0)

void mutation_queue::acquire_row_lock(const mutation_ptr &mu)
{
    for (auto *request : mu->client_requests) {
        if (request == nullptr) {
            continue;
        }

        LOG_INFO("pid={}, rpc_code={}, partition_hash={}",
                 _replica->get_gpid(),
                 request->rpc_code(),
                 request->header->client.partition_hash);

        CHECK_RPC_REQUEST_IS_WRITE(request);

        const auto result = _row_locks.try_emplace(request->header->client.partition_hash, 1);
        if (result.second) {
            // The lock for this hash key is newly created with count 1.
            continue;
        }

        // The lock for this hash key has existed, just increase its count.
        ++result.first->second;
    }
}

void mutation_queue::release_row_lock(const mutation_ptr &mu)
{
    for (auto *request : mu->client_requests) {
        if (request == nullptr) {
            continue;
        }

        CHECK_RPC_REQUEST_IS_WRITE(request);

        const auto result = _row_locks.find(request->header->client.partition_hash);
        if (result == _row_locks.end()) {
            // The lock for this hash key does not exist, no need to release.
            continue;
        }

        if (result->second > 1) {
            // More than 1 write request with the same hash key is in 2PC phase, just
            // decrease the count for the lock.
            --result->second;
            continue;
        }

        // TODO(wangdan): Frequent insertion and deletion of row locks in the hash table may
        // impact performance. In the future, we can introduce a better GC strategy for row
        // locks, such as using an LRU mechanism:
        // - When a lock's count reaches 0, push it to the tail of a linked list.
        // - If the same hash key is written again recently, remove it from the corresponding
        // position in the linked list.
        // - Once the row lock mapping table exceeds a certain threshold, start removing the
        // least recently accessed locks whose count must be 0 from the head of the linked list.
        _row_locks.erase(result);
    }
}

bool mutation_queue::row_locked(const mutation &mu)
{
    for (auto *request : mu.client_requests) {
        if (request == nullptr) {
            continue;
        }

        CHECK_RPC_REQUEST_IS_WRITE(request);

        const auto result = std::as_const(_row_locks).find(request->header->client.partition_hash);
        if (result == _row_locks.end()) {
            continue;
        }

        // Since a better GC strategy would be introduced to improve the performance, we still
        // check if the count has been 0 though currently all row locks in the mapping table
        // must have non-zero count (all locks whose count is 0 were removed when released).
        if (result->second <= 0) {
            continue;
        }

        return true;
    }

    return false;
}

mutation_ptr mutation_queue::try_unblock()
{
    // To erase a node from a singly linked list, we use an extra `prev` to hold the previous
    // node.
    for (auto curr = _blocking_mutations.begin(), prev = _blocking_mutations.before_begin();
         curr != _blocking_mutations.end();) {
        if (row_locked(*curr)) {
            // The mutation that `curr` points to is still being locked. Turn to the next.
            prev = curr++;
            continue;
        }

        // Since now this mutation's memory is managed by `ref_ptr`, its ref count could be
        // released which is originally added in try_block().
        mutation_ptr mu(&(*curr));
        mu->release_ref();

        // Erase the mutation that no longer needs to be blocked from the blocking list.
        (void)_blocking_mutations.erase_after(prev);

        // Increment the count as this mutation will be returned and processed in 2PC phase.
        ++_current_op_count;

        return mu;
    }

    return {};
}

bool mutation_queue::try_block(const mutation_ptr &mu)
{
    if (!mu->is_blocking_candidate || !row_locked(*mu)) {
        ++_current_op_count;
        return false;
    }

    // Since in the intrusive singly linked list the mutation's memory is not managed by
    // `ref_ptr`, add its ref count which will be released while popped from the linked list
    // in try_unblock().
    mu->add_ref();

    // Push the mutation to the tail of the singly linked list without any copy constructor
    // called, in constant time complexity.
    _blocking_mutations.push_back(*mu);

    return true;
}

mutation_ptr mutation_queue::pop_or_block_queue()
{
    while (true) {
        const auto mu = pop_internal_queue();
        if (mu == nullptr) {
            return {};
        }

        if (!try_block(mu)) {
            return mu;
        }
    }
}

mutation_ptr mutation_queue::pop_or_block_pending()
{
    mutation_ptr mu = _pending_mutation;
    _pending_mutation.reset();

    if (!try_block(mu)) {
        return mu;
    }

    return {};
}

mutation_ptr mutation_queue::add_work(message_ex *request)
{
    CHECK_NOTNULL(request, "");

    auto *spec = task_spec::get(request->rpc_code());
    CHECK_NOTNULL(spec, "");

    // If this request is not allowed to be batched, promote `_pending_mutation` if it is
    // non-null. We don't check `_batch_write_disabled` since `_pending_mutation` must be
    // null now if it is true.
    if (_pending_mutation != nullptr && !spec->rpc_request_is_write_allow_batch) {
        promote_pending();
    }

    // Once `_pending_mutation` is cleared, just assign a new mutation to it. If the client
    // request is an atomic write and idempotence is enabled, the new mutation will be created
    // as a blocking candidate.
    if (_pending_mutation == nullptr) {
        _pending_mutation =
            _replica->new_mutation(invalid_decree, _replica->need_make_idempotent(spec));
    }

    LOG_DEBUG("add request with trace_id = {:#018x} into mutation with mutation_tid = {}",
              request->header->trace_id,
              _pending_mutation->tid());

    // Append the incoming client request to `_pending_mutation`.
    _pending_mutation->add_client_request(request);

    // Throttling is triggered as there are too many mutations being processed as 2PC. Return
    // null in case more mutations flow into the write pipeline.
    if (_current_op_count >= _max_concurrent_op) {
        // Since the pending mutation was just filled with the client request, try to promote
        // it.
        try_promote_pending(spec);
        return {};
    }

    // Traverse the blocking mutations to check if someone has become unblocked.
    const auto mu = try_unblock();
    if (mu != nullptr) {
        // Since the pending mutation was just filled with the client request, try to promote
        // it.
        try_promote_pending(spec);
        return mu;
    }

    if (_queue.empty()) {
        // `_pending_mutation` must be non-null now. There's no need to promote it as `_queue`
        // is empty: pop it as the next work candidate to be processed if it is not blocked.
        return pop_or_block_pending();
    }

    // Since the pending mutation was just filled with the client request, try to promote
    // it.
    try_promote_pending(spec);

    // Now the head of `_queue` becomes the head of the entire queue. Try to pop an unblocked
    // mutation from it as the next work candidate to be processed.
    return pop_or_block_queue();
}

mutation_ptr mutation_queue::next_work(int current_running_count)
{
    _current_op_count = current_running_count;

    // Throttling is triggered as there are too many mutations being processed as 2PC. Just
    // return null in case more mutations flow into the write pipeline.
    if (_current_op_count >= _max_concurrent_op) {
        return {};
    }

    // Traverse the blocking mutations to check if someone has become unblocked.
    const auto mu = try_unblock();
    if (mu != nullptr) {
        return mu;
    }

    if (_queue.empty()) {
        // There's not any further work to be processed if `_pending_mutation` is also null.
        if (_pending_mutation == nullptr) {
            return {};
        }

        // `_pending_mutation` is not null now. Just pop it as the next work candidate to be
        // processed if it is not blocked.
        return pop_or_block_pending();
    }

    // Now the head of `_queue` becomes the head of the entire queue. Try to pop an unblocked
    // mutation from it as the next work candidate to be processed.
    return pop_or_block_queue();
}

void mutation_queue::clear()
{
    while (!_blocking_mutations.empty()) {
        // Release the ref count which is originally added in try_block().
        _blocking_mutations.front().release_ref();
        _blocking_mutations.pop_front();
    }

    // Use pop_internal_queue() to clear `_queue` since `_pcount` should also be updated.
    mutation_ptr r;
    while ((r = pop_internal_queue()) != nullptr) {
    }

    if (_pending_mutation != nullptr) {
        _pending_mutation.reset();
    }

    _row_locks.clear();
}

void mutation_queue::clear(std::vector<mutation_ptr> &queued_mutations)
{
    queued_mutations.clear();

    while (!_blocking_mutations.empty()) {
        // Release the ref count which is originally added in try_block().
        queued_mutations.emplace_back(&(_blocking_mutations.front()));
        _blocking_mutations.front().release_ref();
        _blocking_mutations.pop_front();
    }

    // Use pop_internal_queue() to clear `_queue` since `_pcount` should also be updated.
    mutation_ptr r;
    while ((r = pop_internal_queue()) != nullptr) {
        queued_mutations.emplace_back(r);
    }

    if (_pending_mutation != nullptr) {
        queued_mutations.emplace_back(std::move(_pending_mutation));
        _pending_mutation.reset();
    }

    _row_locks.clear();

    // We don't reset the `_current_op_count` here, since it is done by next_work() where the
    // parameter `current_running_count` is specified to reset `_current_op_count` as 0.
}

} // namespace dsn::replication
