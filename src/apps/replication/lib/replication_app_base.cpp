/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus(rDSN) -=- 
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
 
#include "replica.h"
#include "mutation.h"
#include <dsn/internal/factory_store.h>

#define __TITLE__ "TwoPhaseCommit"

namespace dsn { namespace replication {

void register_replica_provider(replica_app_factory f, const char* name)
{
    ::dsn::utils::factory_store<replication_app_base>::register_factory(name, f, PROVIDER_TYPE_MAIN);
}

replication_app_base::replication_app_base(replica* replica, configuration_ptr& config)
{
    _dir = replica->dir();
    _replica = replica;
}

int replication_app_base::write_internal(mutation_ptr& mu, bool ack_client)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");

    int err = 0;
    auto& msg = mu->client_request;
    dispatch_rpc_call(
        static_cast<int>(msg->header().client.port), // hack
        msg,
        ack_client
        );

    if (0 == err)
    {
        dassert(mu->data.header.decree == last_committed_decree(), "");
    }    

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
