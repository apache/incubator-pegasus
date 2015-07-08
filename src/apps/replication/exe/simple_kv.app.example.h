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
# include "simple_kv.client.h"
# include "simple_kv.client.perf.h"
# include "simple_kv.server.h"

namespace dsn { namespace replication { namespace application { 
// client app example
class simple_kv_client_app : public ::dsn::service::service_app, public virtual ::dsn::service::servicelet
{
public:
    simple_kv_client_app(::dsn::service_app_spec* s) 
        : ::dsn::service::service_app(s) 
    {
        _simple_kv_client = nullptr;
    }
    
    ~simple_kv_client_app() 
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        std::vector<::dsn::end_point> meta_servers;
        auto cf = ::dsn::service::system::config();
        ::dsn::replication::replication_app_client_base::load_meta_servers(cf, meta_servers);
        
        _simple_kv_client = new simple_kv_client(meta_servers, argv[1]);
        _timer = ::dsn::service::tasking::enqueue(LPC_SIMPLE_KV_TEST_TIMER, this, &simple_kv_client_app::on_test_timer, 0, 0, 1000);
        return ::dsn::ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        _timer->cancel(true);
 
        if (_simple_kv_client != nullptr)
        {
            delete _simple_kv_client;
            _simple_kv_client = nullptr;
        }
    }

    void on_test_timer()
    {
        char buffer[20];
        sprintf(buffer, "value.%u", env::random32(0, 100));

        std::string value(buffer);

        {
            ::dsn::replication::application::kv_pair req;
            req.key = "key";
            req.value = value;

            //sync:
            int32_t resp;
            auto err = _simple_kv_client->write(req, resp);
            std::cout << "call RPC_SIMPLE_KV_SIMPLE_KV_WRITE end, write " << req.value << ", err = " << err.to_string() << std::endl;
            //async: 
            //_simple_kv_client->begin_write(req);

            std::string v;
            auto err2 = _simple_kv_client->read(req.key, v);
            std::cout << "call RPC_SIMPLE_KV_SIMPLE_KV_READ end, read " << v << ", err = " << err2.to_string() << std::endl;

            if (err == ERR_SUCCESS && err2 == ERR_SUCCESS)
            {
                dassert(v == value, "data is inconsistent!");
            }
        }
    }

private:
    ::dsn::task_ptr _timer;
    simple_kv_client *_simple_kv_client;
};


class simple_kv_perf_test_client_app : public ::dsn::service::service_app, public virtual ::dsn::service::servicelet
{
public:
    simple_kv_perf_test_client_app(::dsn::service_app_spec* s)
        : ::dsn::service::service_app(s)
    {
        _simple_kv_client = nullptr;
    }

    ~simple_kv_perf_test_client_app()
    {
        stop();
    }

    virtual ::dsn::error_code start(int argc, char** argv)
    {
        if (argc < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        std::vector<::dsn::end_point> meta_servers;
        auto cf = ::dsn::service::system::config();
        ::dsn::replication::replication_app_client_base::load_meta_servers(cf, meta_servers);

        _simple_kv_client = new simple_kv_perf_test_client(meta_servers, argv[1]);
        _simple_kv_client->start_test();
        return ::dsn::ERR_SUCCESS;
    }

    virtual void stop(bool cleanup = false)
    {
        if (_simple_kv_client != nullptr)
        {
            delete _simple_kv_client;
            _simple_kv_client = nullptr;
        }
    }
    
private:
    simple_kv_perf_test_client *_simple_kv_client;
};


} } } 
