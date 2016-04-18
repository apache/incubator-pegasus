# pragma once
# include "deploy_svc.code.definition.h"
# include <iostream>

namespace dsn { namespace dist { 
class deploy_svc_service 
    : public ::dsn::serverlet<deploy_svc_service>
{
public:
    deploy_svc_service() : ::dsn::serverlet<deploy_svc_service>("deploy_svc") {}
    virtual ~deploy_svc_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY 
    virtual void on_deploy(const deploy_request& args, ::dsn::rpc_replier<deploy_info>& reply)
    {
        std::cout << "... exec RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY ... (not implemented) " << std::endl;
        deploy_info resp;
        reply(resp);
    }
    // RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY 
    virtual void on_undeploy(const std::string& args, ::dsn::rpc_replier< ::dsn::error_code>& reply)
    {
        std::cout << "... exec RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY ... (not implemented) " << std::endl;
        ::dsn::error_code resp;
        reply(resp);
    }
    // RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST 
    virtual void on_get_service_list(const std::string& args, ::dsn::rpc_replier<deploy_info_list>& reply)
    {
        std::cout << "... exec RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST ... (not implemented) " << std::endl;
        deploy_info_list resp;
        reply(resp);
    }
    // RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO 
    virtual void on_get_service_info(const std::string& args, ::dsn::rpc_replier<deploy_info>& reply)
    {
        std::cout << "... exec RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO ... (not implemented) " << std::endl;
        deploy_info resp;
        reply(resp);
    }
    // RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST 
    virtual void on_get_cluster_list(const std::string& args, ::dsn::rpc_replier<cluster_list>& reply)
    {
        std::cout << "... exec RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST ... (not implemented) " << std::endl;
        cluster_list resp;
        reply(resp);
    }
    
public:
    void open_service(dsn_gpid gpid)
    {
        this->register_async_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY, "deploy", &deploy_svc_service::on_deploy, gpid);
        this->register_async_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY, "undeploy", &deploy_svc_service::on_undeploy, gpid);
        this->register_async_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST, "get_service_list", &deploy_svc_service::on_get_service_list, gpid);
        this->register_async_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO, "get_service_info", &deploy_svc_service::on_get_service_info, gpid);
        this->register_async_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST, "get_cluster_list", &deploy_svc_service::on_get_cluster_list, gpid);
    }

    void close_service(dsn_gpid gpid)
    {
        this->unregister_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY, gpid);
        this->unregister_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY, gpid);
        this->unregister_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST, gpid);
        this->unregister_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO, gpid);
        this->unregister_rpc_handler(RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST, gpid);
    }
};

} } 