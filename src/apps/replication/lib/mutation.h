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

# pragma once


# include "replication_common.h"
# include <list>
# include <dsn/internal/link.h>

#pragma warning(disable: 4201)

namespace dsn { namespace replication {

class mutation : public ref_counter
{
public:
    mutation();
    virtual ~mutation();

    // state inquery
    const char* name() const { return _name; }        
    bool is_logged() const { return _not_logged == 0; }
    bool is_ready_for_commit() const { return _private0 == 0; }
    dsn_message_t prepare_msg() { return _prepare_request; }
    unsigned int left_secondary_ack_count() const { return _left_secondary_ack_count; }
    unsigned int left_potential_secondary_ack_count() const { return _left_potential_secondary_ack_count; }
    ::dsn::task_ptr& log_task() { return _log_task; }
    node_tasks& remote_tasks() { return _prepare_or_commit_tasks; }
    bool is_prepare_close_to_timeout(int gap_ms, int timeout_ms) { return dsn_now_ms() + gap_ms >= _prepare_ts_ms + timeout_ms; }

    // state change
    void set_id(ballot b, decree c);
    bool add_client_request(dsn_task_code_t code, dsn_message_t request);
    void copy_from(mutation_ptr& old);
    void set_logged() { dassert (!is_logged(), ""); _not_logged = 0; }
    unsigned int decrease_left_secondary_ack_count() { return --_left_secondary_ack_count; }
    unsigned int decrease_left_potential_secondary_ack_count() { return --_left_potential_secondary_ack_count; }
    void set_left_secondary_ack_count(unsigned int count) { _left_secondary_ack_count = count; }
    void set_left_potential_secondary_ack_count(unsigned int count) { _left_potential_secondary_ack_count = count; }
    int  clear_prepare_or_commit_tasks();
    int  clear_log_task();
    void set_prepare_ts() { _prepare_ts_ms = dsn_now_ms(); }

    // >= 1 MB
    bool is_full() const { return _appro_data_bytes >= 1024 * 1024; }
    
    // reader & writer
    static mutation_ptr read_from(binary_reader& readeer, dsn_message_t from);
    void write_to(binary_writer& writer);

    // data
    mutation_data  data;

    // user requests
    struct client_info
    {
        int           code;
        dsn_message_t req;
    };
    std::vector<client_info> client_requests;

    // used by pending mutation queue only
    mutation*      next;
        
private:
    union
    {
        struct
        {
            unsigned int _not_logged : 1;
            unsigned int _left_secondary_ack_count : 15;
            unsigned int _left_potential_secondary_ack_count : 16;
        };
        uint32_t _private0;
    };

    uint64_t        _prepare_ts_ms;
    ::dsn::task_ptr _log_task;
    node_tasks      _prepare_or_commit_tasks;
    dsn_message_t   _prepare_request;
    char            _name[40]; // ballot.decree
    int             _appro_data_bytes;
};

class mutation_queue
{
public:
    mutation_queue(global_partition_id gpid, int max_concurrent_op = 2, bool batch_write_disabled = false)
        : _max_concurrent_op(max_concurrent_op), _batch_write_disabled(batch_write_disabled)
    {
        _current_op_count = 0;
        _pending_mutation = nullptr;
        dassert(gpid.app_id != 0, "invalid gpid");
        _pcount = dsn_task_queue_virtual_length_ptr(
            RPC_PREPARE,
            gpid_to_hash(gpid)
            );
    }

    ~mutation_queue()
    {
        clear();
        dassert(_hdr.is_empty(),
            "work queue is deleted when there are still %d running ops or pending work items in queue",
            _current_op_count
            );
    }

    mutation_ptr add_work(int code, dsn_message_t request, replica* r);

    void clear();

    // called when the curren operation is completed,
    // which triggers further round of operations as returned
    mutation_ptr on_work_completed(int current_running_count);

private:
    mutation_ptr unlink_next_workload()
    {
        mutation_ptr r = _hdr.pop_one();
        if (r.get() != nullptr)
        {
            r->release_ref(); // added in add_work        
            *_pcount--;
        }
        return r;
    }

    void reset_max_concurrent_ops(int max_c)
    {
        _max_concurrent_op = max_c;
    }

private:
    int _current_op_count;
    int _max_concurrent_op;
    bool _batch_write_disabled;
    
    volatile int*   _pcount;
    mutation_ptr    _pending_mutation;
    slist<mutation> _hdr;
};

// ---------------------- inline implementation ----------------------------
inline void mutation::set_id(ballot b, decree c)
{
    data.header.ballot = b;
    data.header.decree = c;
    sprintf (_name, "%" PRId64 ".%" PRId64, b, c);
}

}} // namespace

#pragma warning(default: 4201)
