# pragma once
# include <dsn/cpp/service.api.oo.h>
# include "cli.types.h"
# include <iostream>


namespace dsn { 

    DEFINE_TASK_CODE_RPC(RPC_DSN_CLI_CALL, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);

class cli_client 
    : public virtual ::dsn::servicelet
{
public:
    cli_client(const dsn_address_t& server) { _server = server; }
    cli_client() { _server = dsn_address_invalid; }
    virtual ~cli_client() {}


    // ---------- call RPC_DSN_CLI_CALL ------------
    // - synchronous 
    ::dsn::error_code call(
        const command& c, 
        __out_param std::string& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        ::dsn::message_ptr response;

        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DSN_CLI_CALL, c, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            ::unmarshall(response.get(), resp);
        }
        
        return err;
    }
    
    // - asynchronous with on-stack command and std::string 
    ::dsn::task_ptr begin_call(
        const command& c, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DSN_CLI_CALL, 
                    c, 
                    this, 
                    &cli_client::end_call,
                    context,
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_call(
        ::dsn::error_code err, 
        const std::string& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DSN_CLI_CALL err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DSN_CLI_CALL ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<command> and std::shared_ptr<std::string> 
    ::dsn::task_ptr begin_call2(
        std::shared_ptr<command>& c,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const dsn_address_t *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_DSN_CLI_CALL, 
                    c, 
                    this, 
                    &cli_client::end_call2, 
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_call2(
        ::dsn::error_code err, 
        std::shared_ptr<command>& c, 
        std::shared_ptr<std::string>& resp)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DSN_CLI_CALL err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DSN_CLI_CALL ok" << std::endl;
        }
    }
    

private:
    dsn_address_t _server;
};

} 