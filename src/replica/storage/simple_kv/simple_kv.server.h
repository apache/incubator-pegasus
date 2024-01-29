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

#include "replica/replication_app_base.h"
#include "common/storage_serverlet.h"

#include "simple_kv.code.definition.h"
#include "simple_kv_types.h"

namespace dsn {
namespace replication {
namespace application {
class simple_kv_service : public replication_app_base, public storage_serverlet<simple_kv_service>
{
public:
    simple_kv_service(replica *r) : replication_app_base(r) {}
    virtual ~simple_kv_service() {}

    virtual int on_request(dsn::message_ex *request) override WARN_UNUSED_RESULT
    {
        return handle_request(request);
    }

protected:
    // all service handlers to be implemented further
    // RPC_SIMPLE_KV_SIMPLE_KV_READ
    virtual void on_read(const std::string &key, ::dsn::rpc_replier<std::string> &reply)
    {
        std::cout << "... exec RPC_SIMPLE_KV_SIMPLE_KV_READ ... (not implemented) " << std::endl;
        std::string resp;
        reply(resp);
    }
    // RPC_SIMPLE_KV_SIMPLE_KV_WRITE
    virtual void on_write(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
    {
        std::cout << "... exec RPC_SIMPLE_KV_SIMPLE_KV_WRITE ... (not implemented) " << std::endl;
        int32_t resp = 0;
        reply(resp);
    }
    // RPC_SIMPLE_KV_SIMPLE_KV_APPEND
    virtual void on_append(const kv_pair &pr, ::dsn::rpc_replier<int32_t> &reply)
    {
        std::cout << "... exec RPC_SIMPLE_KV_SIMPLE_KV_APPEND ... (not implemented) " << std::endl;
        int32_t resp = 0;
        reply(resp);
    }

    static void register_rpc_handlers()
    {
        register_async_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_READ, "read", on_read);
        register_async_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_WRITE, "write", on_write);
        register_async_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_APPEND, "append", on_append);
    }

private:
    static void
    on_read(simple_kv_service *svc, const std::string &key, dsn::rpc_replier<std::string> &reply)
    {
        svc->on_read(key, reply);
    }
    static void
    on_write(simple_kv_service *svc, const kv_pair &pr, dsn::rpc_replier<int32_t> &reply)
    {
        svc->on_write(pr, reply);
    }
    static void
    on_append(simple_kv_service *svc, const kv_pair &pr, dsn::rpc_replier<int32_t> &reply)
    {
        svc->on_append(pr, reply);
    }
};
}
}
}
