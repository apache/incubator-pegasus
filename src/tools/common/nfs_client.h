# pragma once
# include <dsn/internal/service.api.oo.h>
# include "nfs_code_definition.h"
# include <iostream>


namespace dsn { namespace service { 
class nfs_client 
    : public virtual ::dsn::service::servicelet
{
public:
    nfs_client(const ::dsn::end_point& server) { _server = server; }
    nfs_client() { _server = ::dsn::end_point::INVALID; }
    virtual ~nfs_client() {}


    // ---------- call RPC_NFS_COPY ------------
    // - synchronous 
    ::dsn::error_code copy(
        const copy_request& request, 
        __out_param copy_response& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::end_point *p_server_addr = nullptr)
    {
        ::dsn::message_ptr msg = ::dsn::message::create_request(RPC_NFS_COPY, timeout_milliseconds, hash);
        marshall(msg->writer(), request);
        auto resp_task = ::dsn::service::rpc::call(p_server_addr ? *p_server_addr : _server, msg, nullptr);
        resp_task->wait();
        if (resp_task->error() == ::dsn::ERR_SUCCESS)
        {
            unmarshall(resp_task->get_response()->reader(), resp);
        }
        return resp_task->error();
    }
    
    // - asynchronous with on-stack copy_request and copy_response 
    ::dsn::rpc_response_task_ptr begin_copy(
        const copy_request& request, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::end_point *p_server_addr = nullptr)
    {
        return ::dsn::service::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_NFS_COPY, 
                    request, 
                    this, 
                    &nfs_client::end_copy,
                    context,
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_copy(
        ::dsn::error_code err, 
        const copy_response& resp,
        void* context)
    {
        if (err != ::dsn::ERR_SUCCESS) std::cout << "reply RPC_NFS_COPY err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_COPY ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<copy_request> and std::shared_ptr<copy_response> 
    ::dsn::rpc_response_task_ptr begin_copy2(
        std::shared_ptr<copy_request>& request,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::end_point *p_server_addr = nullptr)
    {
        return ::dsn::service::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_NFS_COPY, 
                    request, 
                    this, 
                    &nfs_client::end_copy2, 
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_copy2(
        ::dsn::error_code err, 
        std::shared_ptr<copy_request>& request, 
        std::shared_ptr<copy_response>& resp)
    {
        if (err != ::dsn::ERR_SUCCESS) std::cout << "reply RPC_NFS_COPY err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_COPY ok" << std::endl;
        }
    }
    

    // ---------- call RPC_NFS_GET_FILE_SIZE ------------
    // - synchronous 
    ::dsn::error_code get_file_size(
        const get_file_size_request& request, 
        __out_param get_file_size_response& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::end_point *p_server_addr = nullptr)
    {
        ::dsn::message_ptr msg = ::dsn::message::create_request(RPC_NFS_GET_FILE_SIZE, timeout_milliseconds, hash);
        marshall(msg->writer(), request);
        auto resp_task = ::dsn::service::rpc::call(p_server_addr ? *p_server_addr : _server, msg, nullptr);
        resp_task->wait();
        if (resp_task->error() == ::dsn::ERR_SUCCESS)
        {
            unmarshall(resp_task->get_response()->reader(), resp);
        }
        return resp_task->error();
    }
    
    // - asynchronous with on-stack get_file_size_request and get_file_size_response 
    ::dsn::rpc_response_task_ptr begin_get_file_size(
        const get_file_size_request& request, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::end_point *p_server_addr = nullptr)
    {
        return ::dsn::service::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_NFS_GET_FILE_SIZE, 
                    request, 
                    this, 
                    &nfs_client::end_get_file_size,
                    context,
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_get_file_size(
        ::dsn::error_code err, 
        const get_file_size_response& resp,
        void* context)
    {
        if (err != ::dsn::ERR_SUCCESS) std::cout << "reply RPC_NFS_GET_FILE_SIZE err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_GET_FILE_SIZE ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<get_file_size_request> and std::shared_ptr<get_file_size_response> 
    ::dsn::rpc_response_task_ptr begin_get_file_size2(
        std::shared_ptr<get_file_size_request>& request,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::end_point *p_server_addr = nullptr)
    {
        return ::dsn::service::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_NFS_GET_FILE_SIZE, 
                    request, 
                    this, 
                    &nfs_client::end_get_file_size2, 
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_get_file_size2(
        ::dsn::error_code err, 
        std::shared_ptr<get_file_size_request>& request, 
        std::shared_ptr<get_file_size_response>& resp)
    {
        if (err != ::dsn::ERR_SUCCESS) std::cout << "reply RPC_NFS_GET_FILE_SIZE err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_GET_FILE_SIZE ok" << std::endl;
        }
    }
    

private:
    ::dsn::end_point _server;
};

} } 