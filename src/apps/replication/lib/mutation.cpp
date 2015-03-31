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
    _memorySize = sizeof(mutation_header);
    _private0 = 0; 
    _notLogged = 1;
}

mutation::~mutation()
{
    clear_log_task();
}

void mutation::add_client_request(message_ptr& request)
{
    dsn::utils::blob buffer(request->reader().get_remaining_buffer());
    auto buf = buffer.buffer();
    utils::blob bb(buf, static_cast<int>(buffer.data() - buffer.buffer().get()), buffer.length());

    client_requests.push_back(request);
    data.updates.push_back(bb);
    _memorySize += request->total_size();
}

/*static*/ mutation_ptr mutation::read_from(message_ptr& reader)
{
    mutation_ptr mu(new mutation());
    unmarshall(reader, mu->data);

    for (auto it = mu->data.updates.begin(); it != mu->data.updates.end(); it++)
    {        
        void * buf = malloc(it->length());
        memcpy(buf, it->data(), it->length());                              
        dsn::utils::blob bb((const char *)buf, 0, it->length());
        message_ptr msg(new message(bb, false));
        mu->client_requests.push_back(msg);
        mu->_memorySize += msg->total_size();
    }

    mu->_fromMessage = reader;
    sprintf (mu->_name, "%llu.%llu", mu->data.header.ballot, mu->data.header.decree);
    return mu;
}

void mutation::write_to(message_ptr& writer)
{
    marshall(writer, data);
}

int mutation::clear_prepare_or_commit_tasks()
{
    int c = 0;
    for (auto it = _prepareOrCommitTasks.begin(); it != _prepareOrCommitTasks.end(); it++)
    {
        it->second->cancel(true);
        c++;
    }

    _prepareOrCommitTasks.clear();
    return c;
}

int mutation::clear_log_task()
{
    if (_logTask != nullptr)
    {
        _logTask->cancel(true);
        _logTask = nullptr;
        return 1;
    }
    else
    {
        return 0;
    }
}

}} // namespace end
