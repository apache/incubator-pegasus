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

mutation::mutation()
{
    _private0 = 0; 
    _not_logged = 1;
    _prepare_ts_ms = 0;
    _prepare_request = nullptr;
    next = nullptr;
    _appro_data_bytes = sizeof(mutation_header);
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
    for (auto& r : client_requests)
    {
        if (r.req)
            dsn_msg_add_ref(r.req);
    }

    if (old->is_logged())
    {
        set_logged();
        data.header.log_offset = old->data.header.log_offset;
    }

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
        unmarshall(reader, ci.code);
        ci.req = nullptr;
        mu->client_requests.push_back(ci);
    }

    if (nullptr != from)
    {
        mu->_prepare_request = from;
        dsn_msg_add_ref(from); // released on dctor
    }
    
    sprintf(mu->_name, "%" PRId64 ".%" PRId64,
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
            marshall(temp_writer, ci.code);
        }
        inserter(temp_writer.get_buffer());
    }
}

void mutation::write_to(binary_writer& writer)
{
    marshall(writer, data);

    for (auto& ci : client_requests)
    {
        marshall(writer, ci.code);
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

mutation_ptr mutation_queue::add_work(int code, dsn_message_t request, replica* r)
{
    // batch and add to work queue
    if (!_pending_mutation)
    {
        _pending_mutation = r->new_mutation(invalid_decree);
    }

    _pending_mutation->add_client_request(code, request);

    // short-cut
    if (_current_op_count < _max_concurrent_op 
        && _hdr.is_empty()
        )
    {
        auto ret = _pending_mutation;
        _pending_mutation = nullptr;
        _current_op_count++;
        return ret;
    }

    // check if full
    if (_batch_write_disabled || _pending_mutation->is_full())
    {
        _pending_mutation->add_ref(); // released when unlink
        _hdr.add(_pending_mutation);
        _pending_mutation = nullptr;
        *_pcount++;
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
        return ret;
    }
    else
    {
        _current_op_count++;
        return unlink_next_workload();
    }
}

mutation_ptr mutation_queue::on_work_completed(int current_running_count)
{
    _current_op_count = current_running_count;

    // no further workload
    if (_hdr.is_empty())
    {
        if (_pending_mutation != nullptr)
        {
            auto ret = _pending_mutation;
            _pending_mutation = nullptr;
            _current_op_count++;
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
