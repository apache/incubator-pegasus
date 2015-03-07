# include "replication_common.h"
# include "rpc_replicated.h"

using namespace rdsn::replication;

namespace rdsn { namespace service {

namespace RpcReplicatedImpl { 

struct Params
{
    end_point localAddr;
    std::vector<end_point> servers;
    service_base* svc;
    rpc_reply_handler callback;
    int reply_hash; 
};

static end_point GetNextServer(const end_point& currentServer, const std::vector<end_point>& servers)
{
    if (currentServer == end_point::INVALID)
    {
        return servers[env::random32(0, (int)servers.size() * 13) % (int)servers.size()];
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
            return servers[env::random32(0, (int)servers.size() * 13) % (int)servers.size()];
        }
    }
}

static void InternalRpcReplyCallback(error_code err, message_ptr& request, message_ptr& response, Params* params)
{
    //printf ("%s\n", __FUNCTION__);

    end_point nextServer;
    if (!err) 
    {
        CdtMsgResponseHeader header;
        unmarshall(response->reader(), header);

        if (header.Err == ERR_SERVICE_NOT_ACTIVE || header.Err == ERR_BUSY)
        {

        }
        else if (header.Err == ERR_TALK_TO_OTHERS)
        {
            nextServer = header.PrimaryAddress;
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

    params->svc->rpc_call(        
        nextServer,
        request,
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
        const end_point& localAddr,
        const end_point& firstTryServer,
        const std::vector<end_point>& servers, 
        message_ptr& request,             

        // reply
        service_base* svc,
        rpc_reply_handler callback, 
        int reply_hash
        )
{
    end_point first = firstTryServer;
    if (first == end_point::INVALID)
    {
        first = RpcReplicatedImpl::GetNextServer(firstTryServer, servers);
    }

    RpcReplicatedImpl::Params *params = new RpcReplicatedImpl::Params;
    params->localAddr = localAddr;
    params->servers = servers;
    params->callback = callback;
    params->reply_hash = reply_hash;
    params->svc = svc;

    return svc->rpc_call(
        first,
        request,
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
        const end_point& localAddr,
        const end_point& firstTryServer,
        const std::vector<end_point>& servers, 
        message_ptr& request,             

        // reply
        service_base* svc,
        rpc_reply_handler callback  
        )
{
    return rpc_replicated(localAddr, firstTryServer, servers, request, svc, callback, 
        request->header().client.hash);
}

}} // end namespace
