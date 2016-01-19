# pragma once

# include "deploy_svc.client.h"

namespace dsn { namespace dist { 
class deploy_svc_perf_test_client
    : public deploy_svc_client,
      public ::dsn::service::perf_client_helper
{
public:
    deploy_svc_perf_test_client(
        ::dsn::rpc_address server)
        : deploy_svc_client(server)
    {
    }

    void start_test()
    {
        perf_test_suite s;
        std::vector<perf_test_suite> suits;

        s.name = "deploy_svc.deploy";
        s.config_section = "task.RPC_DEPLOY_SVC_DEPLOY_SVC_DEPLOY";
        s.send_one = [this](int payload_bytes){this->send_one_deploy(payload_bytes); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);

        s.name = "deploy_svc.undeploy";
        s.config_section = "task.RPC_DEPLOY_SVC_DEPLOY_SVC_UNDEPLOY";
        s.send_one = [this](int payload_bytes){this->send_one_undeploy(payload_bytes); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);

        s.name = "deploy_svc.get_service_list";
        s.config_section = "task.RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_LIST";
        s.send_one = [this](int payload_bytes){this->send_one_get_service_list(payload_bytes); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);

        s.name = "deploy_svc.get_service_info";
        s.config_section = "task.RPC_DEPLOY_SVC_DEPLOY_SVC_GET_SERVICE_INFO";
        s.send_one = [this](int payload_bytes){this->send_one_get_service_info(payload_bytes); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);

        s.name = "deploy_svc.get_cluster_list";
        s.config_section = "task.RPC_DEPLOY_SVC_DEPLOY_SVC_GET_CLUSTER_LIST";
        s.send_one = [this](int payload_bytes){this->send_one_get_cluster_list(payload_bytes); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);

        start(suits);
    }

    void send_one_deploy(int payload_bytes)
    {
        void* ctx = prepare_send_one();
        deploy_request req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();

        begin_deploy(req, ctx, _timeout_ms);
    }

    virtual void end_deploy(
        ::dsn::error_code err,
        deploy_info&& resp,
        void* context) override
    {
        end_send_one(context, err);
    }

    void send_one_undeploy(int payload_bytes)
    {
        void* ctx = prepare_send_one();
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();

        begin_undeploy(req, ctx, _timeout_ms);
    }

    virtual void end_undeploy(
        ::dsn::error_code err,
        ::dsn::error_code&& resp,
        void* context) override
    {
        end_send_one(context, err);
    }

    void send_one_get_service_list(int payload_bytes)
    {
        void* ctx = prepare_send_one();
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();

        begin_get_service_list(req, ctx, _timeout_ms);
    }

    virtual void end_get_service_list(
        ::dsn::error_code err,
        deploy_info_list&& resp,
        void* context) override
    {
        end_send_one(context, err);
    }

    void send_one_get_service_info(int payload_bytes)
    {
        void* ctx = prepare_send_one();
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();

        begin_get_service_info(req, ctx, _timeout_ms);
    }

    virtual void end_get_service_info(
        ::dsn::error_code err,
        deploy_info&& resp,
        void* context) override
    {
        end_send_one(context, err);
    }

    void send_one_get_cluster_list(int payload_bytes)
    {
        void* ctx = prepare_send_one();
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();

        begin_get_cluster_list(req, ctx, _timeout_ms);
    }

    virtual void end_get_cluster_list(
        ::dsn::error_code err,
        cluster_list&& resp,
        void* context) override
    {
        end_send_one(context, err);
    }
};

} } 