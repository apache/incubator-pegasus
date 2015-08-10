/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# pragma once

# include <dsn/service_api_cpp.h>
# include <iostream>

DEFINE_TASK_CODE(LPC_ECHO_TIMER, TASK_PRIORITY_HIGH, ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_RPC(RPC_ECHO, TASK_PRIORITY_HIGH, ::dsn::THREAD_POOL_DEFAULT)

using namespace dsn;
using namespace dsn::service;

class echo_server : public serverlet<echo_server>, public service_app
{
public:
    echo_server()
        : serverlet<echo_server>("echo_server")
    {
        _empty_reply = dsn_config_get_value_bool("apps.server", "empty_reply", false, 
            "whether to send empty reply or echo the whole incoming request");
    }

    void on_echo(const std::string& req, __out_param std::string& resp)
    {
        if (!_empty_reply)
            resp = req;
        else
            resp = "";
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        register_rpc_handler(RPC_ECHO, "RPC_ECHO", &echo_server::on_echo);
        return ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        unregister_rpc_handler(RPC_ECHO);
    }

private:
    bool _empty_reply;
};

class echo_client : public serverlet<echo_client>, public service_app
{
public:
    echo_client()
        : servicelet(8), serverlet<echo_client>("echo_client")
    {
        _message_size = (int)dsn_config_get_value_uint64("apps.client", "message_size", 1024, "message size");
        _concurrency = (int)dsn_config_get_value_uint64("apps.client", "concurrency", 1, "concurrency for the test");
        _bench = dsn_config_get_value_string("apps.client", "bench", "echo", "which benchmark to run");
        _test_local_queue = dsn_config_get_value_bool("apps.client", "queue-test-local", false, "whether queue the calls to current threads");
        
        _seq = 0;
        _last_report_ts_ms = now_ms();
        _recv_bytes_since_last = 0;
        _live_echo_count = 0;
        _timer = nullptr;
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 3)
            return ERR_INVALID_PARAMETERS;

        dsn_address_build(&_server, argv[1], (uint16_t)atoi(argv[2]));

        if (_bench == "echo")
        {
            _timer = tasking::enqueue(LPC_ECHO_TIMER, this, &echo_client::on_echo_timer, 0, 1000);
        }
        else if (_bench == "queue-test")
        {
            uint64_t last_report_ts = now_ms();
            for (int i = 0; i < 16; i++)
            {
                tasking::enqueue(LPC_ECHO_TIMER, this, std::bind(&echo_client::queue_test, this, i, 0, last_report_ts), i, 1000);
            }
        }
        
        return ERR_OK;
    }

    virtual void stop(bool cleanup = false)
    {
        if (nullptr != _timer)
        {
            _timer->cancel(true);
            _timer = nullptr;
        }
    }

    void queue_test(int hash, int count, uint64_t ts_ms)
    {
        if (!_test_local_queue)
        {
            hash = (++hash) % 16;
        }

        ++count;
        //std::cout << hash << " queue-test to " << count << std::endl;

        if (count % 1000000 == 0)
        {
            auto nts = now_ms();
            std::cout << (nts - ts_ms) << " ms elapsed, " <<  hash << " queue-test to " << count << std::endl;
    //        ts_ms = nts;
        }
        
        tasking::enqueue(LPC_ECHO_TIMER, this, std::bind(&echo_client::queue_test, this, hash, count, ts_ms), hash);
    }

    void send_one()
    {
        char buf[120];
        sprintf(buf, "%u", ++_seq);

        std::shared_ptr<std::string> req(new std::string("hi, dsn "));
        *req = req->append(buf);
        req->resize(_message_size);
        rpc::call_typed(_server, RPC_ECHO, req, this, &echo_client::on_echo_reply, 0, 5000);
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
        if (err != ERR_OK)
        {
            bool s = false;
            //std::cout << "echo err: " << err.to_string() << std::endl;
            {
                zauto_lock l(_lock);
                if (0 == --_live_echo_count)
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
                if (n - _last_report_ts_ms >= 10000)
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
        if (err != ERR_OK)
        {
            bool s = false;
            std::cout << "echo err: " << err.to_string() << std::endl;
            {
                zauto_lock l(_lock);
                if (0 == --_live_echo_count)
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

    std::string _bench;
    bool _test_local_queue;
    dsn_address_t _server;
    int _seq;
    int _message_size;
    int _concurrency;
    dsn::task_ptr _timer;
};
