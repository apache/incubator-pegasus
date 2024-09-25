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

#pragma once

#include <stdint.h>

#include "rpc/message_parser.h"
#include "rpc/network.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "task/task_spec.h"
#include "utils/error_code.h"

namespace dsn {
class rpc_engine;

namespace tools {

class sim_network_provider;

class sim_client_session : public rpc_session
{
public:
    sim_client_session(sim_network_provider &net,
                       ::dsn::rpc_address remote_addr,
                       message_parser_ptr &parser);

    void connect() override;

    void send(uint64_t signature) override;

    void do_read(int sz) override {}

    void close() override {}

    void on_failure(bool is_write) override {}
};

class sim_server_session : public rpc_session
{
public:
    sim_server_session(sim_network_provider &net,
                       ::dsn::rpc_address remote_addr,
                       rpc_session_ptr &client,
                       message_parser_ptr &parser);

    void send(uint64_t signature) override;

    void connect() override {}

    void do_read(int sz) override {}

    void close() override {}

    void on_failure(bool is_write) override {}

private:
    rpc_session_ptr _client;
};

class sim_network_provider : public connection_oriented_network
{
public:
    sim_network_provider(rpc_engine *rpc, network *inner_provider);
    ~sim_network_provider(void) {}

    virtual error_code start(rpc_channel channel, int port, bool client_only);

    const ::dsn::rpc_address &address() const override { return _address; }
    const ::dsn::host_port &host_port() const override { return _hp; }

    virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr)
    {
        message_parser_ptr parser(new_message_parser(_client_hdr_format));
        return rpc_session_ptr(new sim_client_session(*this, server_addr, parser));
    }

    virtual rpc_session_ptr create_server_session(::dsn::rpc_address client_addr,
                                                  rpc_session_ptr client_session)
    {
        message_parser_ptr parser(new_message_parser(_client_hdr_format));
        return rpc_session_ptr(new sim_server_session(*this, client_addr, client_session, parser));
    }

    uint32_t net_delay_milliseconds() const;

private:
    ::dsn::rpc_address _address;
    ::dsn::host_port _hp;
};

//------------- inline implementations -------------
} // namespace tools
} // namespace dsn
