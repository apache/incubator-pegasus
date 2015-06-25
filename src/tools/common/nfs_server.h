# pragma once
# include <dsn/serverlet.h>
# include "nfs_code_definition.h"
# include <iostream>

namespace dsn { namespace service { 
class nfs_service 
    : public ::dsn::service::serverlet<nfs_service>
{
public:
    nfs_service() : ::dsn::service::serverlet<nfs_service>("nfs") {}
    virtual ~nfs_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_NFS_COPY 
    virtual void on_copy(const copy_request& request, ::dsn::service::rpc_replier<copy_response>& reply)
    {
        std::cout << "... exec RPC_NFS_COPY ... (not implemented) " << std::endl;
        copy_response resp;
        reply(resp);
    }
    // RPC_NFS_GET_FILE_SIZE 
    virtual void on_get_file_size(const get_file_size_request& request, ::dsn::service::rpc_replier<get_file_size_response>& reply)
    {
        std::cout << "... exec RPC_NFS_GET_FILE_SIZE ... (not implemented) " << std::endl;
        get_file_size_response resp;
        reply(resp);
    }
    
public:
    void open_service()
    {
        this->register_async_rpc_handler(RPC_NFS_COPY, "copy", &nfs_service::on_copy);
        this->register_async_rpc_handler(RPC_NFS_GET_FILE_SIZE, "get_file_size", &nfs_service::on_get_file_size);
    }

    void close_service()
    {
        this->unregister_rpc_handler(RPC_NFS_COPY);
        this->unregister_rpc_handler(RPC_NFS_GET_FILE_SIZE);
    }
};

} } 