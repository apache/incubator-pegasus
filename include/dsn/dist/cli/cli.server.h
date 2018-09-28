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
#include <iostream>
#include <dsn/cpp/serverlet.h>
#include <dsn/dist/cli/cli.code.definition.h>
#include <dsn/dist/cli/cli_types.h>

namespace dsn {
class cli_service : public ::dsn::serverlet<cli_service>
{
public:
    static std::unique_ptr<cli_service> create_service();

public:
    cli_service() : ::dsn::serverlet<cli_service>("cli") {}
    virtual ~cli_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_CLI_CLI_CALL
    virtual void on_call(const command &request, ::dsn::rpc_replier<std::string> &reply)
    {
        std::cout << "... exec RPC_CLI_CLI_CALL ... (not implemented) " << std::endl;
        std::string resp;
        reply(resp);
    }

public:
    void open_service()
    {
        this->register_async_rpc_handler(RPC_CLI_CLI_CALL, "call", &cli_service::on_call);
    }
    void close_service() { this->unregister_rpc_handler(RPC_CLI_CLI_CALL); }
};
}
