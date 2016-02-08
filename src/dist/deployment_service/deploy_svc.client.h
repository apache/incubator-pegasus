# pragma once
# include "deploy_svc.code.definition.h"
# include <iostream>


namespace dsn { namespace dist { 
class deploy_svc_client 
    : public virtual ::dsn::clientlet
{
public:
    deploy_svc_client(::dsn::rpc_address server) { _server = server; }
    deploy_svc_client() { }
    virtual ~deploy_svc_client() {}


    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY ------------
    // - synchronous 
    std::pair< ::dsn::error_code, deploy_info> deploy_sync(
        const deploy_request& req, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<deploy_info>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY,
                req,
                nullptr,
                empty_callback,
                hash,
                timeout
                )
            );
    }
    
    // - asynchronous with on-stack deploy_request and deploy_info 
    template<typename TCallback>
    ::dsn::task_ptr deploy(
        const deploy_request& req, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY, 
                    req, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash
                    );
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY ------------
    // - synchronous 
    std::pair< ::dsn::error_code, ::dsn::error_code> undeploy_sync(
        const std::string& service_url, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap< ::dsn::error_code>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY,
                service_url,
                nullptr,
                empty_callback,
                hash,
                timeout
                )
            );
    }
    
    // - asynchronous with on-stack std::string and ::dsn::error_code 
    template<typename TCallback>
    ::dsn::task_ptr undeploy(
        const std::string& service_url, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY, 
                    service_url, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash
                    );
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST ------------
    // - synchronous 
    std::pair< ::dsn::error_code, deploy_info_list> get_service_list_sync(
        const std::string& package_id, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<deploy_info_list>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST,
                package_id,
                nullptr,
                empty_callback,
                hash,
                timeout
                )
            );
    }
    
    // - asynchronous with on-stack std::string and deploy_info_list 
    template<typename TCallback>
    ::dsn::task_ptr get_service_list(
        const std::string& package_id, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST, 
                    package_id, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash
                    );
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO ------------
    // - synchronous 
    std::pair< ::dsn::error_code, deploy_info> get_service_info_sync(
        const std::string& service_url, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<deploy_info>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO,
                service_url,
                nullptr,
                empty_callback,
                hash,
                timeout
                )
            );
    }
    
    // - asynchronous with on-stack std::string and deploy_info 
    template<typename TCallback>
    ::dsn::task_ptr get_service_info(
        const std::string& service_url, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO, 
                    service_url, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash
                    );
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST ------------
    // - synchronous 
    std::pair< ::dsn::error_code, cluster_list> get_cluster_list_sync(
        const std::string& format, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<cluster_list>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST,
                format,
                nullptr,
                empty_callback,
                hash,
                timeout
                )
            );
    }
    
    // - asynchronous with on-stack std::string and cluster_list 
    template<typename TCallback>
    ::dsn::task_ptr get_cluster_list(
        const std::string& format, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST, 
                    format, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash
                    );
    }

private:
    ::dsn::rpc_address _server;
};

} } 