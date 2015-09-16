# pragma once

# include "echo.client.h"

namespace dsn { namespace example { 

class echo_perf_test_client
    : public echo_client, public ::dsn::service::perf_client_helper
{
public:
    echo_perf_test_client(
        const ::dsn::rpc_address& server)
        : echo_client(server)
    {
    }

    void start_test()
    {
        perf_test_suite s;
        std::vector<perf_test_suite> suits;

        s.name = "echo.ping";
        s.config_section = "task.RPC_ECHO_ECHO_PING";
        s.send_one = [this](int payload_bytes){this->send_one_ping(payload_bytes); };
        s.cases.clear();
        load_suite_config(s);
        suits.push_back(s);
        
        start(suits);
    }                

    void send_one_ping(int payload_bytes)
    {
        void* ctx = prepare_send_one();
        if (!ctx)
            return;

        std::string req;
        req.resize(payload_bytes, 'v');

        // TODO: randomize the value of req
        // auto rs = random64(0, 10000000);
        // std::stringstream ss;
        // ss << "key." << rs;
        // req = ss.str();
        
        begin_ping(req, ctx, _timeout_ms);
    }

    virtual void end_ping(
        ::dsn::error_code err,
        const std::string& resp,
        void* context) override
    {
        end_send_one(context, err);
    }
};

} } 