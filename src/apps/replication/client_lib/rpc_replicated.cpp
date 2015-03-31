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
# include "replication_common.h"
# include "rpc_replicated.h"

using namespace dsn::replication;

namespace dsn { namespace service {

namespace RpcReplicatedImpl { 

struct Params
{
    std::vector<end_point> servers;
    servicelet* svc;
    rpc_reply_handler callback;
    int reply_hash; 
};

static end_point GetNextServer(const end_point& currentServer, const std::vector<end_point>& servers)
{
    if (currentServer == end_point::INVALID)
    {
        return servers[env::random32(0, static_cast<int>(servers.size()) * 13) % static_cast<int>(servers.size())];
    }
    else
    {
        auto it = std::find(servers.begin(), servers.end(), currentServer);
        if (it != servers.end())
        {
            ++it;
            return it == servers.end() ? *servers.begin() : *it;
        }
        else
        {
            return servers[env::random32(0, static_cast<int>(servers.size()) * 13) % static_cast<int>(servers.size())];
        }
    }
}

static void InternalRpcReplyCallback(error_code err, message_ptr& request, message_ptr& response, Params* params)
{
    //printf ("%s\n", __FUNCTION__);

    end_point next_server;
    if (!err) 
    {
        CdtMsgResponseHeader header;
        unmarshall(response->reader(), header);

        if (header.Err == ERR_SERVICE_NOT_ACTIVE || header.Err == ERR_BUSY)
        {

        }
        else if (header.Err == ERR_TALK_TO_OTHERS)
        {
            next_server = header.PrimaryAddress;
            err = ERR_SUCCESS;
        }
        else
        {
            if (nullptr != params->callback)
            {
                (params->callback)(err, request, response);
            }
            delete params;
            return;
        }
    }

    if (err)
    {
        if (nullptr != params->callback)
        {
            (params->callback)(err, request, response);
        }
        delete params;
        return;
    }

    rpc::call(
        next_server,
        request,
        params->svc,
        std::bind(
            &RpcReplicatedImpl::InternalRpcReplyCallback, 
            std::placeholders::_1, 
            std::placeholders::_2, 
            std::placeholders::_3, 
            params),
        params->reply_hash
        );
}


} // end namespace RpcReplicatedImpl 

rpc_response_task_ptr rpc_replicated(
        const end_point& first_server,
        const std::vector<end_point>& servers, 
        message_ptr& request,             

        // reply
        servicelet* svc,
        rpc_reply_handler callback, 
        int reply_hash
        )
{
    end_point first = first_server;
    if (first == end_point::INVALID)
    {
        first = RpcReplicatedImpl::GetNextServer(first_server, servers);
    }

    RpcReplicatedImpl::Params *params = new RpcReplicatedImpl::Params;
    params->servers = servers;
    params->callback = callback;
    params->reply_hash = reply_hash;
    params->svc = svc;

    return rpc::call(
        first,
        request,
        svc,
        std::bind(
        &RpcReplicatedImpl::InternalRpcReplyCallback,
        std::placeholders::_1,
        std::placeholders::_2,
        std::placeholders::_3,
        params),
        reply_hash
        );
}

rpc_response_task_ptr rpc_replicated(
        const end_point& first_server,
        const std::vector<end_point>& servers, 
        message_ptr& request,             

        // reply
        servicelet* svc,
        rpc_reply_handler callback  
        )
{
    return rpc_replicated(first_server, servers, request, svc, callback, 
        request->header().client.hash);
}

}} // end namespace
