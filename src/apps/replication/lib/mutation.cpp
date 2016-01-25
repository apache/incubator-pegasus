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

# include "mutation.h"
# include "mutation_log.h"
# include "replica.h"

namespace dsn { namespace replication {

std::atomic<uint64_t> mutation::s_tid(0);

mutation::mutation()
{
    next = nullptr;
    _private0 = 0; 
    _not_logged = 1;
    _prepare_ts_ms = 0;
    _prepare_request = nullptr;
    strcpy(_name, "0.0.0.0");
    _appro_data_bytes = sizeof(mutation_header);
    _create_ts_ns = dsn_now_ns();
    _tid = ++s_tid;
}

mutation::~mutation()
{
    for (auto& r : client_requests)
    {
        if (r.req)
            dsn_msg_release_ref(r.req);
    }

    if (_prepare_request != nullptr)
    {
        dsn_msg_release_ref(_prepare_request);
    }
}

void mutation::copy_from(mutation_ptr& old)
{
    data.updates = old->data.updates;
    client_requests = old->client_requests;
    _appro_data_bytes = old->_appro_data_bytes;
    _create_ts_ns = old->_create_ts_ns;

    for (auto& r : client_requests)
    {
        if (r.req)
            dsn_msg_add_ref(r.req);
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

    _prepare_request = old->prepare_msg();
    if (_prepare_request)
    {
        dsn_msg_add_ref(_prepare_request);
    }
}

bool mutation::add_client_request(dsn_task_code_t code, dsn_message_t request)
{
    client_info ci;
    ci.code = code;
    ci.req = request;
    client_requests.push_back(ci);

    if (request != nullptr)
    {
        dsn_msg_add_ref(request); // released on dctor

        void* ptr;
        size_t size;
        bool r = dsn_msg_read_next(request, &ptr, &size);
        dassert(r, "payload is not present");
        dsn_msg_read_commit(request, size);

        blob buffer((char*)ptr, 0, (int)size);
        data.updates.push_back(buffer);

        _appro_data_bytes += (int)size + sizeof(int);
    }   
    else
    {
        blob bb;
        data.updates.push_back(bb);

        _appro_data_bytes += sizeof(int);
    }

    dbg_dassert(client_requests.size() == data.updates.size(), 
        "size must be equal");

    return true;
}

/*static*/ mutation_ptr mutation::read_from(binary_reader& reader, dsn_message_t from)
{
    mutation_ptr mu(new mutation());
    unmarshall(reader, mu->data);

    for (auto& d : mu->data.updates)
    {
        client_info ci; 
        std::string code_str;
        unmarshall(reader, code_str);

        ci.code = dsn_task_code_from_string(code_str.c_str(), TASK_CODE_INVALID);
        dassert(ci.code != TASK_CODE_INVALID, "invalid mutation");
        ci.req = nullptr;
        mu->client_requests.push_back(ci);
    }

    if (nullptr != from)
    {
        mu->_prepare_request = from;
        dsn_msg_add_ref(from); // released on dctor
    }
    
    snprintf_p(mu->_name, sizeof(mu->_name),
        "%" PRId32 ".%" PRId32 ".%" PRId64 ".%" PRId64,
        mu->data.header.gpid.app_id,
        mu->data.header.gpid.pidx,
        mu->data.header.ballot,
        mu->data.header.decree);

    return mu;
}

void mutation::write_to_scatter(std::function<void(blob)> inserter) const
{
    {
        binary_writer temp_writer;
        marshall(temp_writer, data.header);
        marshall(temp_writer, data.updates.size());
        for (const auto& bb : data.updates)
        {
            marshall(temp_writer, bb.length());
        }
        inserter(temp_writer.get_buffer());
    }
    
    for (const auto& bb : data.updates)
    {
        inserter(bb);
    }
    {
        binary_writer temp_writer;
        for (auto& ci : client_requests)
        {
            //std::string code_str(dsn_task_code_to_string(ci.code));
            //marshall(temp_writer, code_str);

            // to avoid strcpy in std::string
            const char* cstr = dsn_task_code_to_string(ci.code);
            int len = static_cast<int>(strlen(cstr));
            temp_writer.write_pod(len);
            temp_writer.write(cstr, len);
        }
        inserter(temp_writer.get_buffer());
    }
}

void mutation::write_to(binary_writer& writer)
{
    marshall(writer, data);

    for (auto& ci : client_requests)
    {
        //std::string code_str(dsn_task_code_to_string(ci.code));
        //marshall(writer, code_str);

        // to avoid strcpy in std::string
        const char* cstr = dsn_task_code_to_string(ci.code);
        int len = static_cast<int>(strlen(cstr));
        writer.write_pod(len);
        writer.write(cstr, len);
    }
}

int mutation::clear_prepare_or_commit_tasks()
{
    int c = 0;
    for (auto it = _prepare_or_commit_tasks.begin(); it != _prepare_or_commit_tasks.end(); ++it)
    {
        if (it->second->cancel(true))
        {
            c++;
        }        
    }

    _prepare_or_commit_tasks.clear();
    return c;
}

void mutation::wait_log_task() const
{
    if (_log_task != nullptr)
    {
        _log_task->wait();
    }
}

mutation_queue::mutation_queue(global_partition_id gpid, int max_concurrent_op /*= 2*/, bool batch_write_disabled /*= false*/)
    : _max_concurrent_op(max_concurrent_op), _batch_write_disabled(batch_write_disabled)
{
    std::stringstream ss;
    ss << gpid.app_id << "." << gpid.pidx << "." << "2pc#";

    _current_op_counter.init("eon.replication", ss.str().c_str(), COUNTER_TYPE_NUMBER, "current running 2pc#");
    _current_op_counter.set(0);
    
    _current_op_count = 0;
    _pending_mutation = nullptr;
    dassert(gpid.app_id != 0, "invalid gpid");
    _pcount = dsn_task_queue_virtual_length_ptr(
        RPC_PREPARE,
        gpid_to_hash(gpid)
        );
}

mutation_ptr mutation_queue::add_work(int code, dsn_message_t request, replica* r)
{
    // batch and add to work queue
    if (!_pending_mutation)
    {
        _pending_mutation = r->new_mutation(invalid_decree);
    }

    dinfo("add request with rpc_id=%016lx into mutation with mutation_tid=%" PRIu64,
          dsn_msg_rpc_id(request), _pending_mutation->tid());

    _pending_mutation->add_client_request(code, request);

    // short-cut
    if (_current_op_count < _max_concurrent_op 
        && _hdr.is_empty()
        )
    {
        auto ret = _pending_mutation;
        _pending_mutation = nullptr;
        _current_op_count++;
        _current_op_counter.increment();
        return ret;
    }

    // check if full
    if (_batch_write_disabled || _pending_mutation->is_full())
    {
        _pending_mutation->add_ref(); // released when unlink
        _hdr.add(_pending_mutation);
        _pending_mutation = nullptr;
        ++(*_pcount);
    }
    
    // get next work item
    if (_current_op_count >= _max_concurrent_op)
        return nullptr;
    else if (_hdr.is_empty())
    {
        dassert(_pending_mutation != nullptr, 
            "pending mutation cannot be null");

        auto ret = _pending_mutation;
        _pending_mutation = nullptr;
        _current_op_count++;
        _current_op_counter.increment();
        return ret;
    }
    else
    {
        _current_op_count++;
        _current_op_counter.increment();
        return unlink_next_workload();
    }
}

mutation_ptr mutation_queue::check_possible_work(int current_running_count)
{
    _current_op_count = current_running_count;
    _current_op_counter.set((uint64_t)current_running_count);

    if (_current_op_count >= _max_concurrent_op)
        return nullptr;

    // no further workload
    if (_hdr.is_empty())
    {
        if (_pending_mutation != nullptr)
        {
            auto ret = _pending_mutation;
            _pending_mutation = nullptr;
            _current_op_count++;
            _current_op_counter.increment();
            return ret;
        }
        else
        {
            return nullptr;
        }
    }

    // run further workload
    else
    {
        _current_op_count++;
        return unlink_next_workload();
    }
}

void mutation_queue::clear()
{
    if (_pending_mutation != nullptr)
    {
        _pending_mutation = nullptr;
    }

    mutation_ptr r;
    while ((r = unlink_next_workload()) != nullptr)
    {
    }
}

}} // namespace end
