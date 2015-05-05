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

class mutation : public ref_object
{
public:
    mutation();
    virtual ~mutation();

    // state inquery
    const char* name() const { return _name; }        
    bool is_logged() const { return _not_logged == 0; }
    bool is_prepared() const { return _not_logged == 0; }
    bool is_ready_for_commit() const { return _private0 == 0; }
    message_ptr& owner_message() { return _from_message; }
    unsigned int left_secondary_ack_count() const { return _left_secondary_ack_count; }
    unsigned int left_potential_secondary_ack_count() const { return _left_potential_secondary_ack_count; }
    task_ptr& log_task() { return _log_task; }
    node_tasks& remote_tasks() { return _prepare_or_commit_tasks; }

    // state change
    void set_id(ballot b, decree c);
    void set_client_request(task_code code, message_ptr& request);
    void set_logged() { dassert (!is_logged(), ""); _not_logged = 0; }
    unsigned int decrease_left_secondary_ack_count() { return --_left_secondary_ack_count; }
    unsigned int decrease_left_potential_secondary_ack_count() { return --_left_potential_secondary_ack_count; }
    void set_left_secondary_ack_count(unsigned int count) { _left_secondary_ack_count = count; }
    void set_left_potential_secondary_ack_count(unsigned int count) { _left_potential_secondary_ack_count = count; }
    int  clear_prepare_or_commit_tasks();
    int  clear_log_task();
    
    // reader & writer
    static mutation_ptr read_from(message_ptr& reader);
    void write_to(message_ptr& writer);

    // data
    mutation_data  data;
    message_ptr    client_request;
        
private:
    union
    {
    struct 
    {
    unsigned int _not_logged : 1;
    unsigned int _left_secondary_ack_count : 7;
    unsigned int _left_potential_secondary_ack_count : 8;
    };
    uint16_t       _private0;
    };

    node_tasks    _prepare_or_commit_tasks;
    task_ptr      _log_task;

    message_ptr   _from_message;
    char          _name[40]; // ballot.decree
};

DEFINE_REF_OBJECT(mutation)

// ---------------------- inline implementation ----------------------------
inline void mutation::set_id(ballot b, decree c)
{
    data.header.ballot = b;
    data.header.decree = c;
    sprintf (_name, "%lld.%lld", static_cast<long long int>(b), static_cast<long long int>(c));
}

}} // namespace

#pragma warning(default: 4201)
