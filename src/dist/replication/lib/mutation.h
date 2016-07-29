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
# include <atomic>
# include <dsn/utility/link.h>
# include <dsn/cpp/perf_counter_.h>

#ifndef __linux__
#pragma warning(disable: 4201)
#endif

namespace dsn { namespace replication {

class mutation : public ref_counter
{
public:
    mutation();
    virtual ~mutation();

    // state inquery
    const char* name() const { return _name; }
    const uint64_t tid() const { return _tid; }
    bool is_logged() const { return _not_logged == 0; }
    bool is_ready_for_commit() const { return _private0 == 0; }
    dsn_message_t prepare_msg() { return _prepare_request; }
    unsigned int left_secondary_ack_count() const { return _left_secondary_ack_count; }
    unsigned int left_potential_secondary_ack_count() const { return _left_potential_secondary_ack_count; }
    ::dsn::task_ptr& log_task() { return _log_task; }
    node_tasks& remote_tasks() { return _prepare_or_commit_tasks; }
    bool is_prepare_close_to_timeout(int gap_ms, int timeout_ms) { return dsn_now_ms() + gap_ms >= _prepare_ts_ms + timeout_ms; }
    uint64_t create_ts_ns() const { return _create_ts_ns; }

    // state change
    void set_id(ballot b, decree c);
    void add_client_request(task_code code, dsn_message_t request);
    void copy_from(mutation_ptr& old);
    void set_logged() { dassert (!is_logged(), ""); _not_logged = 0; }
    unsigned int decrease_left_secondary_ack_count() { return --_left_secondary_ack_count; }
    unsigned int decrease_left_potential_secondary_ack_count() { return --_left_potential_secondary_ack_count; }
    void set_left_secondary_ack_count(unsigned int count) { _left_secondary_ack_count = count; }
    void set_left_potential_secondary_ack_count(unsigned int count) { _left_potential_secondary_ack_count = count; }
    int  clear_prepare_or_commit_tasks();
    void wait_log_task() const;
    uint64_t prepare_ts_ms() const { return _prepare_ts_ms; }
    void set_prepare_ts() { _prepare_ts_ms = dsn_now_ms(); }

    // >= 1 MB
    bool is_full() const { return _appro_data_bytes >= 1024 * 1024; }

    // read & write mutation data
    //
    // "mutation_update.code" should be marshalled as string for cross-process compatiblity, because:
    //   - the private log may be transfered to other node with different program
    //   - the private/shared log may be replayed by different program when server restart
    void write_to(std::function<void(const blob&)> inserter) const;
    void write_to(binary_writer& writer, dsn_message_t to) const;
    static mutation_ptr read_from(binary_reader& reader, dsn_message_t from);

    // data
    mutation_data  data;

    // user requests
    std::vector<dsn_message_t> client_requests;

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
    char            _name[60]; // app_id.partition_index.ballot.decree
    int             _appro_data_bytes;
    uint64_t        _create_ts_ns; // for profiling
    uint64_t        _tid; // trace id, unique in process
    static std::atomic<uint64_t> s_tid;
};

class mutation_queue
{
public:
    mutation_queue(gpid gpid, int max_concurrent_op = 2, bool batch_write_disabled = false);

    ~mutation_queue()
    {
        clear();
        dassert(_hdr.is_empty(),
            "work queue is deleted when there are still %d running ops or pending work items in queue",
            _current_op_count
            );
    }

    mutation_ptr add_work(task_code code, dsn_message_t request, replica* r);

    void clear();

    // called when the curren operation is completed or replica configuration is change,
    // which triggers further round of operations as returned
    mutation_ptr check_possible_work(int current_running_count);

private:
    mutation_ptr unlink_next_workload()
    {
        mutation_ptr r = _hdr.pop_one();
        if (r.get() != nullptr)
        {
            r->release_ref(); // added in add_work        
            --(*_pcount);
        }
        return r;
    }

    void reset_max_concurrent_ops(int max_c)
    {
        _max_concurrent_op = max_c;
    }

private:    
    int  _current_op_count;
    int  _max_concurrent_op;
    bool _batch_write_disabled;
    
    volatile int*   _pcount;
    mutation_ptr    _pending_mutation;
    slist<mutation> _hdr;

    perf_counter_  _current_op_counter;
};

// ---------------------- inline implementation ----------------------------
inline void mutation::set_id(ballot b, decree c)
{
    data.header.ballot = b;
    data.header.decree = c;

    snprintf_p(_name, sizeof(_name),
        "%" PRId32 ".%" PRId32 ".%" PRId64 ".%" PRId64,
        data.header.pid.get_app_id(),
        data.header.pid.get_partition_index(),
        data.header.ballot,
        data.header.decree);
}

}} // namespace

#ifndef __linux__
#pragma warning(default: 4201)
#endif
