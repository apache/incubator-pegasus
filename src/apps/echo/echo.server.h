# pragma once
# include "echo.code.definition.h"
# include <iostream>

namespace dsn { namespace example { 
class echo_service 
    : public ::dsn::serverlet<echo_service>
{
public:
    echo_service() : ::dsn::serverlet<echo_service>("echo") {}
    virtual ~echo_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_ECHO_ECHO_PING 
    virtual void on_ping(const std::string& val, ::dsn::rpc_replier<std::string>& reply)
    {
        /*std::cout << "... exec RPC_ECHO_ECHO_PING ... (not implemented) " << std::endl;
        std::string resp;*/
        reply(val);
    }
    
public:
    void open_service()
    {
        this->register_async_rpc_handler(RPC_ECHO_ECHO_PING, "ping", &echo_service::on_ping);
    }

    void close_service()
    {
        this->unregister_rpc_handler(RPC_ECHO_ECHO_PING);
    }
};

} } 