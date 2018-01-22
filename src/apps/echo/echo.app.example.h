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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#pragma once
#include "echo.client.h"
#include "echo.client.perf.h"
#include "echo.server.h"

namespace dsn {
namespace example {
// server app example
class echo_server_app : public ::dsn::service_app
{
public:
    echo_server_app(const ::dsn::service_app_info *info) : ::dsn::service_app(info) {}

    virtual ::dsn::error_code start(const std::vector<std::string> &args)
    {
        _echo_svc.open_service();
        return ::dsn::ERR_OK;
    }

    virtual ::dsn::error_code stop(bool cleanup = false)
    {
        _echo_svc.close_service();
        return ::dsn::ERR_OK;
    }

private:
    echo_service _echo_svc;
};

// client app example
class echo_client_app : public ::dsn::service_app, public virtual ::dsn::clientlet
{
public:
    echo_client_app(const service_app_info *info) : ::dsn::service_app(info) {}

    ~echo_client_app() { stop(); }

    virtual ::dsn::error_code start(const std::vector<std::string> &args)
    {
        if (args.size() < 3)
            return ::dsn::ERR_INVALID_PARAMETERS;

        _server.assign_ipv4(args[1].c_str(), (uint16_t)atoi(args[2].c_str()));
        _echo_client.reset(new echo_client(_server));
        _timer = ::dsn::tasking::enqueue_timer(
            LPC_ECHO_TEST_TIMER, this, [this] { on_test_timer(); }, std::chrono::seconds(1));
        return ::dsn::ERR_OK;
    }

    virtual ::dsn::error_code stop(bool cleanup = false)
    {
        _timer->cancel(true);

        _echo_client.reset();

        return ::dsn::ERR_OK;
    }

    void on_test_timer()
    {
        // test for service 'echo'
        {
            // sync:
            auto result = _echo_client->ping_sync({});
            std::cout << "call RPC_ECHO_ECHO_PING end, return " << result.first.to_string()
                      << std::endl;
            // async:
            //_echo_client->begin_ping(req);
        }
    }

private:
    ::dsn::task_ptr _timer;
    ::dsn::rpc_address _server;

    std::unique_ptr<echo_client> _echo_client;
};

class echo_perf_test_client_app : public ::dsn::service_app, public virtual ::dsn::clientlet
{
public:
    echo_perf_test_client_app(const service_app_info *info) : ::dsn::service_app(info)
    {
        _echo_client = nullptr;
    }

    ~echo_perf_test_client_app() { stop(); }

    virtual ::dsn::error_code start(const std::vector<std::string> &args)
    {
        if (args.size() < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        _server.assign_ipv4(args[1].c_str(), (uint16_t)atoi(args[2].c_str()));

        _echo_client = new echo_perf_test_client(_server);
        _echo_client->start_test("echo.perf-test.case", 1);
        return ::dsn::ERR_OK;
    }

    virtual ::dsn::error_code stop(bool cleanup = false)
    {
        if (_echo_client != nullptr) {
            delete _echo_client;
            _echo_client = nullptr;
        }

        return ::dsn::ERR_OK;
    }

private:
    echo_perf_test_client *_echo_client;
    ::dsn::rpc_address _server;
};
}
}
