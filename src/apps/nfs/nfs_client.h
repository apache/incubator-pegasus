# pragma once
# include "nfs_code_definition.h"
# include <iostream>


namespace dsn { namespace service { 
class nfs_client 
    : public virtual ::dsn::servicelet
{
public:
    nfs_client(const dsn_address_t& server) { _server = server; }
    nfs_client() { _server = dsn_address_invalid; }
    virtual ~nfs_client() {}


    // ---------- call RPC_NFS_COPY ------------
    // - synchronous 
    ::dsn::error_code copy(
        const copy_request& request, 
        __out_param copy_response& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        ::dsn::message_ptr response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_NFS_COPY, request, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            ::unmarshall(response.get(), resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack copy_request and copy_response 
    ::dsn::task_ptr begin_copy(
        const copy_request& request, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
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
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_NFS_COPY err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_COPY ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<copy_request> and std::shared_ptr<copy_response> 
    ::dsn::task_ptr begin_copy2(
        std::shared_ptr<copy_request>& request,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
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
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_NFS_COPY err : " << err.to_string() << std::endl;
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
        const dsn_address_t *p_server_addr = nullptr)
    {
        ::dsn::message_ptr response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_NFS_GET_FILE_SIZE, request, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            ::unmarshall(response.get(), resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack get_file_size_request and get_file_size_response 
    ::dsn::task_ptr begin_get_file_size(
        const get_file_size_request& request, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
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
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_NFS_GET_FILE_SIZE err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_GET_FILE_SIZE ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<get_file_size_request> and std::shared_ptr<get_file_size_response> 
    ::dsn::task_ptr begin_get_file_size2(
        std::shared_ptr<get_file_size_request>& request,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
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
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_NFS_GET_FILE_SIZE err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_NFS_GET_FILE_SIZE ok" << std::endl;
        }
    }
    

private:
    dsn_address_t _server;
};

} } 