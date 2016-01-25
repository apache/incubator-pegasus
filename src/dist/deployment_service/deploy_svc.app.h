# pragma once
# include "deploy_svc.client.h"
# include "deploy_svc.client.perf.h"
# include "deploy_svc.server.impl.h"

namespace dsn { namespace dist { 
// server app example
class deploy_svc_server_app : 
    public ::dsn::service_app
{
public:
    deploy_svc_server_app()
    {}

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        _deploy_svc_svc.open_service();
        return _deploy_svc_svc.start();
    }

    virtual void stop(bool cleanup = false)
    {
        _deploy_svc_svc.close_service();
    }

private:
    deploy_svc_service_impl _deploy_svc_svc;
};

// client app example
class deploy_svc_client_app : 
    public ::dsn::service_app, 
    public virtual ::dsn::clientlet
{
public:
    deploy_svc_client_app() 
    {
        _deploy_svc_client = nullptr;
    }
    
    ~deploy_svc_client_app() 
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 3)
            return ::dsn::ERR_INVALID_PARAMETERS;

        _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));
        _deploy_svc_client = new deploy_svc_client(_server);
        _timer = ::dsn::tasking::enqueue_timer(LPC_DEPLOY_SVC_TEST_TIMER, this, [this] {on_test_timer();}, std::chrono::seconds(1));
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        _timer->cancel(true);
 
        if (_deploy_svc_client != nullptr)
        {
            delete _deploy_svc_client;
            _deploy_svc_client = nullptr;
        }
    }

    void on_test_timer()
    {
        // test for service 'deploy_svc'
        {
            //sync:
            auto result = _deploy_svc_client->deploy_sync({});
            std::cout << "call RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY end, return " << result.first.to_string() << std::endl;
            //async: 
            //_deploy_svc_client->begin_deploy(req);
           
        }
        {
            //sync:
            auto result = _deploy_svc_client->undeploy_sync({});
            std::cout << "call RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY end, return " << result.first.to_string() << std::endl;
            //async: 
            //_deploy_svc_client->begin_undeploy(req);
           
        }
        {
            //sync:
            auto result = _deploy_svc_client->get_service_list_sync({});
            std::cout << "call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST end, return " << result.first.to_string() << std::endl;
            //async: 
            //_deploy_svc_client->begin_get_service_list(req);
           
        }
        {
            //sync:
            auto result = _deploy_svc_client->get_service_info_sync({});
            std::cout << "call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO end, return " << result.first.to_string() << std::endl;
            //async: 
            //_deploy_svc_client->begin_get_service_info(req);
           
        }
        {
            //sync:
            auto result = _deploy_svc_client->get_cluster_list_sync({});
            std::cout << "call RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST end, return " << result.first.to_string() << std::endl;
            //async: 
            //_deploy_svc_client->begin_get_cluster_list(req);
           
        }
    }

private:
    ::dsn::task_ptr _timer;
    ::dsn::rpc_address _server;
    
    deploy_svc_client *_deploy_svc_client;
};

class deploy_svc_perf_test_client_app :
    public ::dsn::service_app, 
    public virtual ::dsn::clientlet
{
public:
    deploy_svc_perf_test_client_app()
    {
        _deploy_svc_client = nullptr;
    }

    ~deploy_svc_perf_test_client_app()
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        _server.assign_ipv4(argv[1], (uint16_t)atoi(argv[2]));

        _deploy_svc_client = new deploy_svc_perf_test_client(_server);
        _deploy_svc_client->start_test();
        return ::dsn::ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        if (_deploy_svc_client != nullptr)
        {
            delete _deploy_svc_client;
            _deploy_svc_client = nullptr;
        }
    }
    
private:
    deploy_svc_perf_test_client *_deploy_svc_client;
    ::dsn::rpc_address _server;
};

} } 