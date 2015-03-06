#pragma once

//
// replication_app_client_base is the base class for clients for 
// all app to be replicated using this library
// 

#include "replication_common.h"
#include <rdsn/serviceletex.h>

namespace rdsn { namespace replication {

DEFINE_ERR_CODE(ERR_REPLICATION_FAILURE)
    
class replication_app_client_base : public rdsn::service::serviceletex<replication_app_client_base>
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
        read_semantic semantic = ReadOutdated,
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
    virtual end_point get_read_address(read_semantic semantic, const partition_configuration& config);

    void clear_all_pending_tasks();
    const end_point& address() const { return _local_address; }

private:
    void _internal_rpc_reply_handler(error_code err, message_ptr& request, message_ptr& response);
    error_code  get_address(int pidx, bool isWrite, __out end_point& addr, __out int& appId, read_semantic semantic = read_semantic::ReadLastUpdate);
    void query_partition_configuration(int pidx);
    void query_partition_configuration_reply(error_code err, message_ptr& request, message_ptr& response, int pidx);
    int  send_client_message(message_ptr& msg, rpc_response_task_ptr& reply, bool firstTime);
    void enqueue_pending_list(int pidx, message_ptr& userRequest, rpc_response_task_ptr& caller_tsk);
    void on_user_request_timeout(rpc_response_task_ptr caller_tsk);
    void calculate_send_time(message_ptr& request, int &maxTime, int &maxCount);

private:
    std::string                            _app_name;
    end_point                            _local_address;
    std::vector<end_point>               _meta_servers;

    mutable rdsn::service::zlock           _lock;
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