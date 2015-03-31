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
#pragma once

//
// replication_app_client_base is the base class for clients for 
// all app to be replicated using this library
// 

#include "replication_common.h"
#include <dsn/serverlet.h>

namespace dsn { namespace replication {

DEFINE_ERR_CODE(ERR_REPLICATION_FAILURE)
    
class replication_app_client_base : public dsn::service::serverlet<replication_app_client_base>
{    
public:
    replication_app_client_base(        
        const std::vector<end_point>& meta_servers, 
        const char* appServiceName, 
        int32_t appServiceId = -1,
        int32_t coordinatorRpcCallTimeoutMillisecondsPerSend = 2000,
        int32_t coordinatorRpcCallMaxSendCount = 3,
        const end_point* pLocalAddr = nullptr);

    ~replication_app_client_base();

    message_ptr create_write_request(
        int partition_index
        );

    message_ptr create_read_request(        
        int partition_index,
        read_semantic_t semantic = ReadOutdated,
        decree snapshot_decree = invalid_decree // only used when ReadSnapshot        
        );

    void set_target_app_server(message_ptr& request, const end_point& server_addr);

    rpc_response_task_ptr send(
        message_ptr& request,        
        int timeout_milliseconds,
      
        rpc_reply_handler callback,
        int reply_hash = 0
        );

    // get read address policy
    virtual end_point get_read_address(read_semantic_t semantic, const partition_configuration& config);

    void clear_all_pending_tasks();

private:
    void _internal_rpc_reply_handler(error_code err, message_ptr& request, message_ptr& response);
    error_code  get_address(int pidx, bool isWrite, __out_param end_point& addr, __out_param int& appId, read_semantic_t semantic = read_semantic_t::ReadLastUpdate);
    void query_partition_configuration(int pidx);
    void query_partition_configuration_reply(error_code err, message_ptr& request, message_ptr& response, int pidx);
    int  send_client_message(message_ptr& msg, rpc_response_task_ptr& reply, bool firstTime);
    void enqueue_pending_list(int pidx, message_ptr& userRequest, rpc_response_task_ptr& caller_tsk);
    void on_user_request_timeout(rpc_response_task_ptr caller_tsk);
    void calculate_send_time(message_ptr& request, int &maxTime, int &maxCount);

private:
    std::string                            _app_name;
    std::vector<end_point>               _meta_servers;

    mutable dsn::service::zlock           _lock;
    std::map<int,  partition_configuration> _config_cache;
    int                                    _app_id;
    end_point                            _last_contact_point;
    
    class pending_message
    {
    public:
        task_ptr         timeout_tsk;
        message_ptr      msg;
        rpc_response_task_ptr caller_tsk;
    };
    // <partition index, <cdt query task, <pending msg>>>
    typedef std::map<int, std::pair<rpc_response_task_ptr, std::list<pending_message>* > > pending_messages;
    pending_messages  _pending_messages;  

    int32_t          _meta_server_rpc_call_timeout_milliseconds_per_send;
    int32_t          _meta_server_rpc_call_max_send_count;
};
}} // namespace
