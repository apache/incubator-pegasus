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
#include "simple_kv.client.h"
#include "simple_kv.server.h"

namespace dsn {
namespace replication {
namespace application {
// client app example
class simple_kv_client_app : public ::dsn::service_app
{
public:
    simple_kv_client_app(const service_app_info *info) : ::dsn::service_app(info) {}

    virtual ~simple_kv_client_app() override { stop(); }

    virtual ::dsn::error_code start(const std::vector<std::string> &args)
    {
        if (args.size() < 2)
            return ::dsn::ERR_INVALID_PARAMETERS;

        printf("%s %s %s\n", args[1].c_str(), args[2].c_str(), args[3].c_str());
        dsn::rpc_address meta;
        meta.from_string_ipv4(args[2].c_str());
        _simple_kv_client.reset(new simple_kv_client(args[1].c_str(), {meta}, args[3].c_str()));

        _timer = ::dsn::tasking::enqueue_timer(LPC_SIMPLE_KV_TEST_TIMER,
                                               &_tracker,
                                               [this] { on_test_timer(); },
                                               std::chrono::seconds(1));
        return ::dsn::ERR_OK;
    }

    virtual ::dsn::error_code stop(bool cleanup = false)
    {
        _tracker.cancel_outstanding_tasks();

        _simple_kv_client.reset();

        return ::dsn::ERR_OK;
    }

    void on_test_timer()
    {
        // test for service simple_kv        using namespace svc_simple_kv;
        {
            std::string req = "hello";
            // sync:
            error_code err;
            std::string resp;
            std::tie(err, resp) = _simple_kv_client->read_sync(req);
            std::cout << "call RPC_SIMPLE_KV_SIMPLE_KV_READ end, return " << err.to_string();
            if (ERR_OK == err)
                std::cout << ", read result: " << resp;
            std::cout << std::endl;
            // async:
            //_simple_kv_client->read(req, empty_rpc_handler);
        }
        {
            kv_pair req;
            req.key = "hello";
            req.value = "world";
            // sync:
            error_code err;
            int32_t resp;
            std::tie(err, resp) = _simple_kv_client->write_sync(req);
            std::cout << "call RPC_SIMPLE_KV_SIMPLE_KV_WRITE end, return " << err.to_string()
                      << std::endl;
            // async:
            //_simple_kv_client->write(req, empty_rpc_handler);
        }
        {
            kv_pair req;
            req.key = "hello";
            req.value = "world";
            // sync:
            error_code err;
            int32_t resp;
            std::tie(err, resp) = _simple_kv_client->append_sync(req);
            std::cout << "call RPC_SIMPLE_KV_SIMPLE_KV_APPEND end, return " << err.to_string()
                      << std::endl;

            // async:
            //_simple_kv_client->append(req, empty_rpc_handler);
        }
    }

private:
    ::dsn::task_ptr _timer;
    ::dsn::rpc_address _server;
    std::unique_ptr<simple_kv_client> _simple_kv_client;
    dsn::task_tracker _tracker;
};
} // namespace application
} // namespace replication
} // namespace dsn
