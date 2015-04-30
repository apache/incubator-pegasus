/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "mutation.h"


namespace dsn { namespace replication {

mutation::mutation()
{
    _private0 = 0; 
    _not_logged = 1;
}

mutation::~mutation()
{
    clear_log_task();
}

void mutation::set_client_request(task_code code, message_ptr& request)
{
    ::dsn::blob buffer(request->reader().get_remaining_buffer());
    auto buf = buffer.buffer();
    blob bb(buf, static_cast<int>(buffer.data() - buffer.buffer().get()), buffer.length());

    client_request = request;
    request->header().client.port = static_cast<uint16_t>(code); // hack
    data.updates.push_back(bb);
}

/*static*/ mutation_ptr mutation::read_from(message_ptr& reader)
{
    mutation_ptr mu(new mutation());
    unmarshall(reader, mu->data);

    for (auto it = mu->data.updates.begin(); it != mu->data.updates.end(); it++)
    {        
        void * buf = malloc(it->length());
        memcpy(buf, it->data(), it->length());                              
        ::dsn::blob bb((const char *)buf, 0, it->length());
        message_ptr msg(new message(bb, false));
        mu->client_request = msg;
    }

    mu->_from_message = reader;
    sprintf (mu->_name, "%lld.%lld", 
            static_cast<long long int>(mu->data.header.ballot),
            static_cast<long long int>(mu->data.header.decree));
    return mu;
}

void mutation::write_to(message_ptr& writer)
{
    marshall(writer, data);
}

int mutation::clear_prepare_or_commit_tasks()
{
    int c = 0;
    for (auto it = _prepare_or_commit_tasks.begin(); it != _prepare_or_commit_tasks.end(); it++)
    {
        it->second->cancel(true);
        c++;
    }

    _prepare_or_commit_tasks.clear();
    return c;
}

int mutation::clear_log_task()
{
    if (_log_task != nullptr)
    {
        _log_task->cancel(true);
        _log_task = nullptr;
        return 1;
    }
    else
    {
        return 0;
    }
}

}} // namespace end
