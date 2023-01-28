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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "mutation.h"
#include "mutation_log.h"
#include "replica.h"
#include "utils/fmt_logging.h"
#include "utils/flags.h"

namespace dsn {
namespace replication {

DSN_DEFINE_uint64(replication,
                  abnormal_write_trace_latency_threshold,
                  1000 * 1000 * 1000, // 1s
                  "latency trace will be logged when exceed the write latency threshold");
DSN_TAG_VARIABLE(abnormal_write_trace_latency_threshold, FT_MUTABLE);

std::atomic<uint64_t> mutation::s_tid(0);

mutation::mutation()
{
    next = nullptr;
    _private0 = 0;
    _not_logged = 1;
    _prepare_ts_ms = 0;
    strcpy(_name, "0.0.0.0");
    _appro_data_bytes = sizeof(mutation_header);
    _create_ts_ns = dsn_now_ns();
    _tid = ++s_tid;
    _is_sync_to_child = false;
    _tracer = std::make_shared<dsn::utils::latency_tracer>(
        false, "mutation", FLAGS_abnormal_write_trace_latency_threshold);
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

void mutation::add_client_request(task_code code, dsn::message_ex *request)
{
    data.updates.push_back(mutation_update());
    mutation_update &update = data.updates.back();
    _appro_data_bytes += 32; // approximate code size

    if (request != nullptr) {
        update.code = code;
        update.serialization_type =
            (dsn_msg_serialize_format)request->header->context.u.serialize_format;
        update.__set_start_time_ns(dsn_now_ns());
        request->add_ref(); // released on dctor

        void *ptr;
        size_t size;
        CHECK(request->read_next(&ptr, &size), "payload is not present");
        request->read_commit(0); // so we can re-read the request buffer in replicated app
        update.data.assign((char *)ptr, 0, (int)size);

        _appro_data_bytes += sizeof(int) + (int)size; // data size
    } else {
        update.code = RPC_REPLICATION_WRITE_EMPTY;
        _appro_data_bytes += sizeof(int); // empty data size
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

mutation_queue::mutation_queue(gpid gpid,
                               int max_concurrent_op /*= 2*/,
                               bool batch_write_disabled /*= false*/)
    : _max_concurrent_op(max_concurrent_op), _batch_write_disabled(batch_write_disabled)
{
    _current_op_count = 0;
    _pending_mutation = nullptr;
    CHECK_NE_MSG(gpid.get_app_id(), 0, "invalid gpid");
    _pcount = dsn_task_queue_virtual_length_ptr(RPC_PREPARE, gpid.thread_hash());
}

mutation_ptr mutation_queue::add_work(task_code code, dsn::message_ex *request, replica *r)
{
    task_spec *spec = task_spec::get(code);

    // if not allow write batch, switch work queue
    if (_pending_mutation && !spec->rpc_request_is_write_allow_batch) {
        _pending_mutation->add_ref(); // released when unlink
        _hdr.add(_pending_mutation);
        _pending_mutation = nullptr;
        ++(*_pcount);
    }

    // add to work queue
    if (!_pending_mutation) {
        _pending_mutation = r->new_mutation(invalid_decree);
    }

    LOG_DEBUG("add request with trace_id = {:#018x} into mutation with mutation_tid = {}",
              request->header->trace_id,
              _pending_mutation->tid());

    _pending_mutation->add_client_request(code, request);

    // short-cut
    if (_current_op_count < _max_concurrent_op && _hdr.is_empty()) {
        auto ret = _pending_mutation;
        _pending_mutation = nullptr;
        _current_op_count++;
        return ret;
    }

    // check if need to switch work queue
    if (_batch_write_disabled || !spec->rpc_request_is_write_allow_batch ||
        _pending_mutation->is_full()) {
        _pending_mutation->add_ref(); // released when unlink
        _hdr.add(_pending_mutation);
        _pending_mutation = nullptr;
        ++(*_pcount);
    }

    // get next work item
    if (_current_op_count >= _max_concurrent_op)
        return nullptr;
    else if (_hdr.is_empty()) {
        CHECK_NOTNULL(_pending_mutation, "pending mutation cannot be null");

        auto ret = _pending_mutation;
        _pending_mutation = nullptr;
        _current_op_count++;
        return ret;
    } else {
        _current_op_count++;
        return unlink_next_workload();
    }
}

mutation_ptr mutation_queue::check_possible_work(int current_running_count)
{
    _current_op_count = current_running_count;

    if (_current_op_count >= _max_concurrent_op)
        return nullptr;

    // no further workload
    if (_hdr.is_empty()) {
        if (_pending_mutation != nullptr) {
            auto ret = _pending_mutation;
            _pending_mutation = nullptr;
            _current_op_count++;
            return ret;
        } else {
            return nullptr;
        }
    }

    // run further workload
    else {
        _current_op_count++;
        return unlink_next_workload();
    }
}

void mutation_queue::clear()
{
    if (_pending_mutation != nullptr) {
        _pending_mutation = nullptr;
    }

    mutation_ptr r;
    while ((r = unlink_next_workload()) != nullptr) {
    }
}

void mutation_queue::clear(std::vector<mutation_ptr> &queued_mutations)
{
    mutation_ptr r;
    queued_mutations.clear();
    while ((r = unlink_next_workload()) != nullptr) {
        queued_mutations.emplace_back(r);
    }

    if (_pending_mutation != nullptr) {
        queued_mutations.emplace_back(std::move(_pending_mutation));
        _pending_mutation = nullptr;
    }

    // we don't reset the current_op_count, coz this is handled by
    // check_possible_work. In which, the variable current_running_count
    // is handled by prepare_list
    // _current_op_count = 0;
}
}
} // namespace end
