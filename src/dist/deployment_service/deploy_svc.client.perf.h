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

    virtual void send_one(int payload_bytes, int key_space_size, const std::vector<double>& ratios) override
    {
        return;
    }

    void send_one_deploy(int payload_bytes, int key_space_size)
    {
        deploy_request req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000) % key_space_size;
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        deploy(
            req,
            [this, context = prepare_send_one()](error_code err, deploy_info&& resp)
            {
                end_send_one(context, err);
            },
            _timeout
            );
    }

    void send_one_undeploy(int payload_bytes, int key_space_size)
    {
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000) % key_space_size;
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        undeploy(
            req,
            [this, context = prepare_send_one()](error_code err, ::dsn::error_code&& resp)
            {
                end_send_one(context, err);
            },
            _timeout
            );
    }

    void send_one_get_service_list(int payload_bytes, int key_space_size)
    {
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000) % key_space_size;
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        get_service_list(
            req,
            [this, context = prepare_send_one()](error_code err, deploy_info_list&& resp)
            {
                end_send_one(context, err);
            },
            _timeout
            );
    }

    void send_one_get_service_info(int payload_bytes, int key_space_size)
    {
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000) % key_space_size;
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        get_service_info(
            req,
            [this, context = prepare_send_one()](error_code err, deploy_info&& resp)
            {
                end_send_one(context, err);
            },
            _timeout
            );
    }

    void send_one_get_cluster_list(int payload_bytes, int key_space_size)
    {
        std::string req;
        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000) % key_space_size;
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        get_cluster_list(
            req,
            [this, context = prepare_send_one()](error_code err, cluster_list&& resp)
            {
                end_send_one(context, err);
            },
            _timeout
            );
    }
};

} } 