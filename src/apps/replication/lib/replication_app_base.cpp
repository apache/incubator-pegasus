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

#include "replica.h"
#include "mutation.h"
#include "replication_app_base.h"

#define __TITLE__ "TwoPhaseCommit"

namespace dsn { namespace replication {

replication_app_base::replication_app_base(replica* replica, const replication_app_config* config)
{
    _dir = replica->dir();
    _replica = replica;
    _last_committed_decree = 0;
    _last_durable_decree = 0;
}

int replication_app_base::write_internal(mutation_ptr& mu, bool ack_client)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");

    int err = 0;
    for (auto& msg : mu->client_requests)
    {
        dispatch_rpc_call(
            msg->header().client.timeout_milliseconds, // hack
            msg,
            ack_client
            );
    }

    ++_last_committed_decree;    
    return err;
}

void replication_app_base::dispatch_rpc_call(int code, message_ptr& request, bool ack_client)
{
    auto it = _handlers.find(code);
    if (it != _handlers.end())
    {
        if (ack_client)
        {
            message_ptr response = request->create_response();
            int err = 0;
            marshall(response->writer(), err);
            it->second(request, response);
        }
        else
        {
            message_ptr response(nullptr);
            it->second(request, response);
        }
    }
    else if (ack_client)
    {
        message_ptr response = request->create_response();
        error_code err = ERR_HANDLER_NOT_FOUND;
        marshall(response->writer(), (int)err);
        rpc::reply(response);
    }
}

}} // end namespace
