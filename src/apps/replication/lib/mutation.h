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


#include "replication_common.h"
#include <list>

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
    dsn_message_t client_msg() { return _client_request; }
    unsigned int left_secondary_ack_count() const { return _left_secondary_ack_count; }
    unsigned int left_potential_secondary_ack_count() const { return _left_potential_secondary_ack_count; }
    ::dsn::task_ptr& log_task() { return _log_task; }
    node_tasks& remote_tasks() { return _prepare_or_commit_tasks; }
    bool is_prepare_close_to_timeout(int gap_ms, int timeout_ms) { return dsn_now_ms() + gap_ms >= _prepare_ts_ms + timeout_ms; }

    // state change
    void set_id(ballot b, decree c);
    void set_client_request(dsn_task_code_t code, dsn_message_t request);
    void copy_from(mutation_ptr& old);
    void set_logged() { dassert (!is_logged(), ""); _not_logged = 0; }
    unsigned int decrease_left_secondary_ack_count() { return --_left_secondary_ack_count; }
    unsigned int decrease_left_potential_secondary_ack_count() { return --_left_potential_secondary_ack_count; }
    void set_left_secondary_ack_count(unsigned int count) { _left_secondary_ack_count = count; }
    void set_left_potential_secondary_ack_count(unsigned int count) { _left_potential_secondary_ack_count = count; }
    int  clear_prepare_or_commit_tasks();
    int  clear_log_task();
    void set_prepare_ts() { _prepare_ts_ms = dsn_now_ms(); }
    
    // reader & writer
    static mutation_ptr read_from(binary_reader& readeer, dsn_message_t from);
    void write_to(binary_writer& writer);

    // data
    mutation_data  data;
    int            rpc_code;
        
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
    dsn_message_t   _client_request;
    char            _name[40]; // ballot.decree
};

// ---------------------- inline implementation ----------------------------
inline void mutation::set_id(ballot b, decree c)
{
    data.header.ballot = b;
    data.header.decree = c;
    sprintf (_name, "%lld.%lld", static_cast<long long int>(b), static_cast<long long int>(c));
}

}} // namespace

#pragma warning(default: 4201)
