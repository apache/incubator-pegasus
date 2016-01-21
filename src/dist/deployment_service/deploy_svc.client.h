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
    ::dsn::error_code deploy(
        const deploy_request& req, 
        /*out*/ deploy_info& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY, req, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack deploy_request and deploy_info 
    ::dsn::task_ptr begin_deploy(
        const deploy_request& req, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY, 
                    req, 
                    this, 
                    [=](error_code err, deploy_info&& resp)
                    {
                        deploy_svc_client::end_deploy(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_deploy(
        ::dsn::error_code err, 
        deploy_info&& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY ok" << std::endl;
        }
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY ------------
    // - synchronous 
    ::dsn::error_code undeploy(
        const std::string& service_url, 
        /*out*/ ::dsn::error_code& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY, service_url, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack std::string and ::dsn::error_code 
    ::dsn::task_ptr begin_undeploy(
        const std::string& service_url, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY, 
                    service_url, 
                    this, 
                    [=](error_code err, ::dsn::error_code&& resp)
                    {
                        deploy_svc_client::end_undeploy(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_undeploy(
        ::dsn::error_code err, 
        ::dsn::error_code&& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY ok" << std::endl;
        }
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST ------------
    // - synchronous 
    ::dsn::error_code get_service_list(
        const std::string& package_id, 
        /*out*/ deploy_info_list& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST, package_id, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack std::string and deploy_info_list 
    ::dsn::task_ptr begin_get_service_list(
        const std::string& package_id, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST, 
                    package_id, 
                    this, 
                    [=](error_code err, deploy_info_list&& resp)
                    {
                        deploy_svc_client::end_get_service_list(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_get_service_list(
        ::dsn::error_code err, 
        deploy_info_list&& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST ok" << std::endl;
        }
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO ------------
    // - synchronous 
    ::dsn::error_code get_service_info(
        const std::string& service_url, 
        /*out*/ deploy_info& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO, service_url, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack std::string and deploy_info 
    ::dsn::task_ptr begin_get_service_info(
        const std::string& service_url, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO, 
                    service_url, 
                    this, 
                    [=](error_code err, deploy_info&& resp)
                    {
                        deploy_svc_client::end_get_service_info(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_get_service_info(
        ::dsn::error_code err, 
        deploy_info&& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO ok" << std::endl;
        }
    }

    // ---------- call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST ------------
    // - synchronous 
    ::dsn::error_code get_cluster_list(
        const std::string& format, 
        /*out*/ cluster_list& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST, format, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack std::string and cluster_list 
    ::dsn::task_ptr begin_get_cluster_list(
        const std::string& format, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST, 
                    format, 
                    this, 
                    [=](error_code err, cluster_list&& resp)
                    {
                        deploy_svc_client::end_get_cluster_list(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_get_cluster_list(
        ::dsn::error_code err, 
        cluster_list&& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST ok" << std::endl;
        }
    }

private:
    ::dsn::rpc_address _server;
};

} } 