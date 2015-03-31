/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# pragma once

# include <dsn/serverlet.h>
# include <dsn/internal/serialization.h>
# include <iostream>

DEFINE_THREAD_POOL_CODE(THREAD_POOL_TEST)
DEFINE_TASK_CODE(LPC_ECHO_TIMER, ::dsn::TASK_PRIORITY_HIGH, THREAD_POOL_TEST)
DEFINE_TASK_CODE_RPC(RPC_ECHO, ::dsn::TASK_PRIORITY_HIGH, THREAD_POOL_TEST)
DEFINE_TASK_CODE_RPC(RPC_ECHO2, ::dsn::TASK_PRIORITY_HIGH, THREAD_POOL_TEST)

using namespace dsn;
using namespace dsn::service;

class echo_server : public serverlet<echo_server>, public service_app
{
public:
    echo_server(service_app_spec* s, configuration_ptr c)
        : service_app(s, c), serverlet<echo_server>("echo_server")
    {
        _empty_reply = config()->get_value<bool>("apps.server", "empty_reply", false);
    }

    void on_echo(const std::string& req, __out_param std::string& resp)
    {
        if (!_empty_reply)
            resp = req;
        else
            resp = "";
    }

    void on_echo2(const blob& req, rpc_replier<blob>& reply)
    {
        if (!_empty_reply)
            reply(req);
        else
        {
            blob empty;
            reply(empty);
        }
    }

    virtual error_code start(int argc, char** argv)
    {
        register_rpc_handler(RPC_ECHO, "RPC_ECHO", &echo_server::on_echo);
        register_async_rpc_handler(RPC_ECHO2, "RPC_ECHO2", &echo_server::on_echo2);
        return ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        unregister_rpc_handler(RPC_ECHO);
        unregister_rpc_handler(RPC_ECHO2);
    }

private:
    bool _empty_reply;
};

class echo_client : public serverlet<echo_client>, public service_app
{
public:
    echo_client(service_app_spec* s, configuration_ptr c)
        : service_app(s, c), serverlet<echo_client>("echo_client")
    {
        _message_size = config()->get_value<int>("apps.client", "message_size", 1024);
        _concurrency = config()->get_value<int>("apps.client", "concurrency", 1);
        _echo2 = config()->get_value<bool>("apps.client", "echo2", false);
        

        _seq = 0;
        _last_report_ts_ms = now_ms();
        _recv_bytes_since_last = 0;
        _live_echo_count = 0;
    }

    virtual error_code start(int argc, char** argv)
    {
        if (argc < 3)
            return ERR_INVALID_PARAMETERS;

        _server = end_point(argv[1], (uint16_t)atoi(argv[2]));
        _timer = tasking::enqueue(LPC_ECHO_TIMER, this, &echo_client::on_echo_timer, 0, 1000);
        return ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        _timer->cancel(true);
    }

    void send_one()
    {
        char buf[120];
        sprintf(buf, "%u", ++_seq);

        if (!_echo2)
        {
            std::shared_ptr<std::string> req(new std::string("hi, dsn "));
            *req = req->append(buf);
            req->resize(_message_size);
            rpc::call_typed(_server, RPC_ECHO, req, this, &echo_client::on_echo_reply, 0, 5000);
        }
        else
        {
            std::shared_ptr<char> buffer((char*)::malloc(_message_size));
            std::shared_ptr<blob> bb(new blob(buffer, _message_size));
            rpc::call_typed(_server, RPC_ECHO2, bb, this, &echo_client::on_echo_reply2, 0, 5000);
        }
    }

    void on_echo_timer()
    {
        for (int i = 0; i < _concurrency; i++)
        {
            {
                zauto_lock l(_lock);
                ++_live_echo_count;
            }
            send_one();
        }
    }

    void on_echo_reply(error_code err, std::shared_ptr<std::string>& req, std::shared_ptr<std::string>& resp)
    {
        if (err != ERR_SUCCESS)
        {
            bool s = false;
            std::cout << "echo err: " << err.to_string() << std::endl;
            {
                zauto_lock l(_lock);
                if (1 == --_live_echo_count)
                {
                    ++_live_echo_count;                    
                    s = true;
                }                
            }

            if (s) send_one();
        }
        else
        {
            {
                zauto_lock l(_lock);
                _recv_bytes_since_last += _message_size;
                auto n = now_ms();
                if (n - _last_report_ts_ms >= 1000)
                {
                    std::cout << "throughput = "
                        << static_cast<double>(_recv_bytes_since_last) / 1024.0 / 1024.0 / ((static_cast<double>(n - _last_report_ts_ms)) / 1000.0)
                        << " MB/s" << std::endl;
                    _last_report_ts_ms = n;
                    _recv_bytes_since_last = 0;
                }
            }

            send_one();
        }        
    }

    void on_echo_reply2(error_code err, std::shared_ptr<blob>& req, std::shared_ptr<blob>& resp)
    {
        if (err != ERR_SUCCESS)
        {
            bool s = false;
            std::cout << "echo err: " << err.to_string() << std::endl;
            {
                zauto_lock l(_lock);
                if (1 == --_live_echo_count)
                {
                    ++_live_echo_count;
                    s = true;
                }
            }

            if (s) send_one();
        }
        else
        {
            {
                zauto_lock l(_lock);
                _recv_bytes_since_last += _message_size;
                auto n = now_ms();
                if (n - _last_report_ts_ms >= 1000)
                {
                    std::cout << "throughput = "
                        << static_cast<double>(_recv_bytes_since_last) / 1024.0 / 1024.0 / ((static_cast<double>(n - _last_report_ts_ms)) / 1000.0)
                        << " MB/s" << std::endl;
                    _last_report_ts_ms = n;
                    _recv_bytes_since_last = 0;
                }
            }

            send_one();
        }
    }

private:
    zlock _lock;
    uint64_t _recv_bytes_since_last;
    uint64_t _last_report_ts_ms;
    int32_t  _live_echo_count;

    end_point _server;
    int _seq;
    int _message_size;
    int _concurrency;
    bool _echo2;
    task_ptr _timer;
};
