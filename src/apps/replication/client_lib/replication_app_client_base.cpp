/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
#include "replication_app_client_base.h"
#include "rpc_replicated.h"

namespace rdsn { namespace replication {

    using namespace ::rdsn::service;

class RepClientMessage : public message
{
public:
    RepClientMessage() // For write Mode.            
    {
        Pidx = -1;
        read_semantic = read_semantic::ReadOutdated;
        ReadSnapshotDecree = invalid_decree;
        HeaderPlaceholderPos = 0;

        TargetServer = end_point::INVALID;

        Callback = nullptr;
    }

    virtual ~RepClientMessage()
    {
    }

    static RepClientMessage* CreateClientRequest(task_code rpc_code, int hash = 0)
    {
        RepClientMessage* msg = new RepClientMessage();

        msg->header().local_rpc_code = (uint16_t)rpc_code;
        msg->header().client.hash = hash;
        msg->header().client.timeout_milliseconds = 0;

        const char* rpcName = rpc_code.to_string();
        strcpy(msg->header().rpc_name, rpcName);    

        msg->header().id = message::new_id();
        
        return msg;
    }
    
public:
    int          Pidx;
    read_semantic read_semantic;
    decree       ReadSnapshotDecree;
    uint16_t     HeaderPlaceholderPos;

    uint64_t         AppSendTimeMs;
    end_point  TargetServer;
    
    rpc_reply_handler Callback;
};
    
replication_app_client_base::replication_app_client_base(const std::vector<end_point>& meta_servers, 
                                                   const char* appServiceName, 
                                                   int32_t appServiceId /*= -1*/,
                                                   int32_t coordinatorRpcCallTimeoutMillisecondsPerSend,
                                                   int32_t coordinatorRpcCallMaxSendCount,
                                                   const end_point* pLocalAddr /*= nullptr*/)
: rdsn::service::serviceletex<replication_app_client_base>(std::string(appServiceName).append(".client").c_str())
{
    _app_name = std::string(appServiceName);   
    _meta_servers = meta_servers;

    _app_id = appServiceId;
    _last_contact_point = end_point::INVALID;

    _meta_server_rpc_call_timeout_milliseconds_per_send = coordinatorRpcCallTimeoutMillisecondsPerSend;
    _meta_server_rpc_call_max_send_count = coordinatorRpcCallMaxSendCount;

    //PerformanceCounters::init(PerfCounters_ReplicationClientBegin, PerfCounters_ReplicationClientEnd);
}

replication_app_client_base::~replication_app_client_base()
{
    clear_all_pending_tasks();
}

void replication_app_client_base::clear_all_pending_tasks()
{
    service::zauto_lock l(_lock);
    for (auto it = _pending_messages.begin(); it != _pending_messages.end(); it++)
    {
        if (it->second.first != nullptr) it->second.first->cancel(false);

        dbg_rassert (it->second.second != nullptr);
        for (auto it2 = it->second.second->begin(); it2 != it->second.second->end(); it2++)
        {
            it2->timeout_tsk->cancel(false);
        }
        delete it->second.second;
    }
    _pending_messages.clear();
}

message_ptr replication_app_client_base::create_write_request(int partition_index)
{
    auto msg = RepClientMessage::CreateClientRequest(RPC_REPLICATION_CLIENT_WRITE);
    msg->Pidx = partition_index;
    msg->HeaderPlaceholderPos = msg->write_placeholder();
    return message_ptr(msg);
}

message_ptr replication_app_client_base::create_read_request(
    int partition_index,
    read_semantic semantic /*= ReadOutdated*/,
    decree snapshot_decree /*= invalid_decree*/ // only used when ReadSnapshot
    )
{
    auto msg = RepClientMessage::CreateClientRequest(RPC_REPLICATION_CLIENT_READ, _meta_server_rpc_call_timeout_milliseconds_per_send);
    msg->Pidx = partition_index;
    msg->read_semantic = semantic;
    msg->ReadSnapshotDecree = snapshot_decree;
    msg->HeaderPlaceholderPos = msg->write_placeholder();
    return message_ptr(msg);
}

void replication_app_client_base::set_target_app_server(message_ptr& request, const end_point& server_addr)
{
    RepClientMessage * msg = (RepClientMessage*)request.get();
    msg->TargetServer = server_addr;
}

void replication_app_client_base::enqueue_pending_list(int pidx, message_ptr& userRequest, rpc_response_task_ptr& caller_tsk)
{
    //RepClientMessage * msg = (RepClientMessage *)userRequest.get();
    pending_message pm;
    pm.timeout_tsk = enqueue_task(
            LPC_TEST,
            std::bind(&replication_app_client_base::on_user_request_timeout, this, caller_tsk),
            0,
            userRequest->header().client.timeout_milliseconds
            );
    pm.msg = userRequest;
    pm.caller_tsk = caller_tsk;
    
    {
    rdsn::service::zauto_lock l(_lock);
    auto it = _pending_messages.find(pidx);
    if (it != _pending_messages.end())
    {
        dbg_rassert (it->second.second != nullptr);
        it->second.second->push_back(pm);
    }
    else
    {
        std::list<pending_message>* msgList = new std::list<pending_message>();
        msgList->push_back(pm);
        _pending_messages.insert(std::make_pair(pidx, std::make_pair((rpc_response_task_ptr)nullptr, msgList)));
    }
    }
}

rpc_response_task_ptr replication_app_client_base::send(
    message_ptr& request,    
    int timeout_milliseconds,

    rpc_reply_handler callback,
    int reply_hash
    )
{
    RepClientMessage* msg = (RepClientMessage*)request.get();
    msg->header().client.timeout_milliseconds = timeout_milliseconds;

    msg->Callback = callback;

    rpc_reply_handler handler(nullptr);
    if (callback != nullptr)
    {
        handler = std::bind(&replication_app_client_base::_internal_rpc_reply_handler, this, 
            std::placeholders::_1, 
            std::placeholders::_2, 
            std::placeholders::_3);
    }

    rpc_response_task_ptr task(new service_rpc_response_task(request, this, handler, reply_hash));

    int err = send_client_message(request, task, true);
    if (err != ERR_SUCCESS)
    {
        //task->enqueue(nullptr, timeoutMillisecondsPerSend * maxSendCount);
        enqueue_pending_list(msg->Pidx, request, task);
        query_partition_configuration(msg->Pidx);
    }

    return task;
}

int replication_app_client_base::send_client_message(message_ptr& msg2, rpc_response_task_ptr& reply, bool firstTime)
{
    RepClientMessage* msg = (RepClientMessage*)msg2.get();

    end_point addr;
    global_partition_id gpid;

    error_code err = ERR_SUCCESS;
    if (msg->TargetServer == end_point::INVALID)
    {
        err = get_address(msg->Pidx, msg->header().local_rpc_code == RPC_REPLICATION_CLIENT_WRITE, addr, gpid.tableId, msg->read_semantic);
    }
    else
    {
        if (_app_id != -1)
        {
            addr = msg->TargetServer;
            gpid.tableId = _app_id;
        }
        else
        {
            err = ERR_IO_PENDING; // to get appId later
        }
    }

    if (err == ERR_SUCCESS)
    {
        gpid.pidx = msg->Pidx;

        if (msg->header().local_rpc_code == RPC_REPLICATION_CLIENT_READ)
        {
            client_read_request req;
            req.gpid = gpid;
            req.semantic = msg->read_semantic;
            req.versionDecree = msg->ReadSnapshotDecree;
            
            marshall(msg2, req, msg->HeaderPlaceholderPos);   
        }
        else
        {
            marshall(msg2, gpid, msg->HeaderPlaceholderPos);            
        }

        msg->header().client.hash = gpid_to_hash(gpid);
        rpc::call(addr, msg2, reply);
    }
    else if (!firstTime)
    {
        message_ptr nil(nullptr);
        reply->enqueue(err, nil);
    }
    return err;
}

void replication_app_client_base::_internal_rpc_reply_handler(
    error_code err,
    message_ptr& request,
    message_ptr& response
    )
{
    RepClientMessage* msg = (RepClientMessage*)request.get();

    if (err != ERR_SUCCESS)
    {
        query_partition_configuration(msg->Pidx);
    }
    else
    {
        int err2;
        response->read(err2);
        if (err2 != 0)
        {
            err = ERR_REPLICATION_FAILURE;
        }
    }

    if (msg->Callback != nullptr)
    {
        msg->Callback(err, request, response);
    }

    // Zhenyu: throttling control using the parameter from client response
}

void replication_app_client_base::on_user_request_timeout(rpc_response_task_ptr caller_tsk)
{
    message_ptr nil(nullptr);
    caller_tsk->enqueue(ERR_TIMEOUT, nil);
}

error_code replication_app_client_base::get_address(int pidx, bool isWrite, __out_param end_point& addr, __out_param int& appId, read_semantic semantic)
{
    error_code err;
    partition_configuration config;
     
    {
    zauto_lock l(_lock);
    auto it = _config_cache.find(pidx);
    if (it != _config_cache.end())
    {
        err = ERR_SUCCESS;
        config = it->second;
    }
    else
    {
        err = ERR_IO_PENDING;
    }
    }

    if (err == ERR_SUCCESS)
    {
        appId = _app_id;
        if (isWrite)
        {
            addr = config.primary;
        }
        else
        {
            addr = get_read_address(semantic, config);
        }

        if (rdsn::end_point::INVALID == addr)
        {
            err = ERR_IO_PENDING;
        }
    } 
    return err;
}

void replication_app_client_base::query_partition_configuration(int pidx)
{            
    rdsn::service::zauto_lock l(_lock);    

    auto it = _pending_messages.find(pidx);
    if (it != _pending_messages.end())
    {
        if (it->second.first != nullptr) return;
    }
    else
    {
        it = _pending_messages.insert(pending_messages::value_type(std::make_pair(pidx, std::make_pair((rpc_response_task_ptr)nullptr, new std::list<pending_message>())))).first;
    }

    message_ptr msg = message::create_request(RPC_CM_CALL, _meta_server_rpc_call_timeout_milliseconds_per_send);

    CdtMsgHeader hdr;
    hdr.RpcTag = RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX;
    marshall(msg, hdr);

    QueryConfigurationByIndexRequest req;
    req.app_name = _app_name;
    req.parIdxes.push_back((uint32_t)pidx);
    
    marshall(msg, req);

    it->second.first = rpc_replicated(
        address(),
        _last_contact_point,
        _meta_servers, 
        msg,            
        this,
        std::bind(&replication_app_client_base::query_partition_configuration_reply, this, 
        std::placeholders::_1, 
        std::placeholders::_2, 
        std::placeholders::_3, pidx)
        );
}

void replication_app_client_base::query_partition_configuration_reply(error_code err, message_ptr& request, message_ptr& response, int pidx)
{
    if (!err)
    {
        QueryConfigurationByIndexResponse resp;
        unmarshall(response, resp);
        
        int err2 = resp.err;

        if (err2 == ERR_SUCCESS) 
        {
            zauto_lock l(_lock);
            _last_contact_point = response->header().from_address;

            if (resp.partitions.size() > 0)
            {
                if (_app_id != -1 && _app_id != resp.partitions[0].gpid.tableId)
                {
                    rassert(false, "App id is changed (mostly the given app id is incorrect), local Vs remote: %u vs %u ", _app_id, resp.partitions[0].gpid.tableId);
                }

                _app_id = resp.partitions[0].gpid.tableId;
            }

            for (auto it = resp.partitions.begin(); it != resp.partitions.end(); it++)
            {
                partition_configuration& newConfig = *it;
                auto it2 = _config_cache.find(newConfig.gpid.pidx);
                if (it2 == _config_cache.end())
                {
                    _config_cache[newConfig.gpid.pidx] = newConfig;
                }
                else if (it2->second.ballot < newConfig.ballot)
                {
                    it2->second = newConfig;
                }
            }
        }
    }
        
    // send pending client msgs
    std::list<pending_message> * messageList = nullptr;
    {
        zauto_lock l(_lock);
        auto it = _pending_messages.find(pidx);
        if (it != _pending_messages.end())
        {
            messageList = it->second.second;
            _pending_messages.erase(pidx);
        }
    }

    if (messageList != nullptr)
    {
        for (auto itr_msg = messageList->begin(); itr_msg != messageList->end(); ++itr_msg)
        {        
            if (itr_msg->timeout_tsk->cancel(false))
            {
                if (ERR_SUCCESS == err)
                {
                    send_client_message(itr_msg->msg, itr_msg->caller_tsk, false);
                }
                else
                {
                    message_ptr nil;
                    itr_msg->caller_tsk->enqueue(err, nil);
                }
            }
        }
    }
    if (nullptr != messageList) delete messageList;
}

end_point replication_app_client_base::get_read_address(read_semantic semantic, const partition_configuration& config)
{
    if (semantic == read_semantic::ReadLastUpdate)
        return config.primary;

    // readsnapshot or readoutdated, using random
    else
    {
        bool hasPrimary = false;
        int N = (int)config.secondaries.size();
        if (config.primary != rdsn::end_point::INVALID)
        {
            N++;
            hasPrimary = true;
        }

        if (0 == N) return config.primary;

        int r = random32(0, 1000) % N;
        if (hasPrimary && r == N - 1)
            return config.primary;
        else
            return config.secondaries[r];
    }
}

}} // end namespace
