# pragma once

# include <rdsn/serviceletex.h>
# include <iostream>

DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST)
DEFINE_TASK_CODE(LPC_ECHO_TIMER, ::rdsn::TASK_PRIORITY_HIGH, THREAD_POOL_TEST)
DEFINE_TASK_CODE_RPC(RPC_ECHO, ::rdsn::TASK_PRIORITY_HIGH, THREAD_POOL_TEST)

using namespace rdsn;
using namespace rdsn::service;

class echo_server : public serviceletex<echo_server>
{
public:
    echo_server() : serviceletex<echo_server>("echo_server")
    {
        register_rpc_handler(RPC_ECHO, "RPC_ECHO", &echo_server::on_echo);
    }

    void on_echo(const std::string& req, __out std::string& resp)
    {
        resp = req;
    }
};

class echo_server_app : public service_app
{
public:
    echo_server_app(service_app_spec* s, configuration_ptr c)
        : service_app(s, c)
    {
        _server = nullptr;
    }

    virtual ~echo_server_app(void)
    {
    }

    virtual error_code start(int argc, char** argv)
    {
        _server = new echo_server();
        return ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        delete _server;
        _server = nullptr;
    }

private:
    echo_server *_server;
};

class echo_client : public serviceletex<echo_client>
{
public:
    echo_client(const char* host, uint16_t port, int message_size) : serviceletex<echo_client>("echo_client")
    {
        _seq = 0;
        _message_size = message_size;
        _server = end_point(host, port);
        enqueue_task(LPC_ECHO_TIMER, &echo_client::on_echo_timer, 0, 0, 1000);
    }

    void on_echo_timer()
    {
        char buf[120];
        sprintf(buf, "%u", ++_seq);
        boost::shared_ptr<std::string> req(new std::string("hi, rdsn "));
        *req = req->append(buf);
        req->resize(_message_size);
        rpc_typed(_server, RPC_ECHO, req, &echo_client::on_echo_reply, 0, 3000);
    }

    void on_echo_reply(error_code err, boost::shared_ptr<std::string> req, boost::shared_ptr<std::string> resp)
    {
        if (err != ERR_SUCCESS) std::cout << "echo err: " << err.to_string() << std::endl;
        else
        {
            std::cout << "echo result: " << resp->c_str() << "(len = " << resp->length() << ")" << std::endl;
        }
    }

private:
    end_point _server;
    int _seq;
    int _message_size;
};

class echo_client_app : public service_app
{
public:
    echo_client_app(service_app_spec* s, configuration_ptr c)
        : service_app(s, c)
    {
        _client = nullptr;
    }

    virtual ~echo_client_app(void)
    {
    }

    virtual error_code start(int argc, char** argv)
    {
        if (argc < 3)
            return ERR_INVALID_PARAMETERS;

        int sz = config()->get_value<int>("apps.client", "message_size", 1024);
        _client = new echo_client(argv[1], (uint16_t)atoi(argv[2]), sz);
        return ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        delete _client;
        _client = nullptr;
    }

private:
    echo_client *_client;
};
