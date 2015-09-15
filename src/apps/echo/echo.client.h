# pragma once
# include "echo.code.definition.h"
# include <iostream>


namespace dsn { namespace example { 
class echo_client 
    : public virtual ::dsn::clientlet
{
public:
    echo_client(const ::dsn::rpc_address& server) { _server = server; }    
    echo_client() { }
    virtual ~echo_client() {}


    // ---------- call RPC_ECHO_ECHO_PING ------------
    // - synchronous 
    ::dsn::error_code ping(
        const std::string& val, 
        /*out*/ std::string& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        ::dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_ECHO_ECHO_PING, val, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack std::string and std::string 
    ::dsn::task_ptr begin_ping(
        const std::string& val, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_ECHO_ECHO_PING, 
                    val, 
                    this, 
                    &echo_client::end_ping,
                    context,
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_ping(
        ::dsn::error_code err, 
        const std::string& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK)
        {
            //std::cout << "reply RPC_ECHO_ECHO_PING err : " << err.to_string() << std::endl;
        }
        else
        {
            //std::cout << "reply RPC_ECHO_ECHO_PING ok" << std::endl;
        }
    }
    
    // - asynchronous with on-heap std::shared_ptr<std::string> and std::shared_ptr<std::string> 
    ::dsn::task_ptr begin_ping2(
        std::shared_ptr<std::string>& val,         
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_ECHO_ECHO_PING, 
                    val, 
                    this, 
                    &echo_client::end_ping2, 
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_ping2(
        ::dsn::error_code err, 
        std::shared_ptr<std::string>& val, 
        std::shared_ptr<std::string>& resp)
    {
        if (err != ::dsn::ERR_OK)
        {
            //std::cout << "reply RPC_ECHO_ECHO_PING err : " << err.to_string() << std::endl;
        }
        else
        {
            //std::cout << "reply RPC_ECHO_ECHO_PING ok" << std::endl;
        }
    }
    

private:
    ::dsn::rpc_address _server;
};

} } 