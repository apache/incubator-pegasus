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
 *     Replication testing framework.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "client.h"
# include "case.h"
# include <dsn/dist/replication/replication_other_types.h>

# include <sstream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "simple_kv.client"

namespace dsn { namespace replication { namespace test {

simple_kv_client_app::simple_kv_client_app() : _simple_kv_client(nullptr)
{
}

simple_kv_client_app::~simple_kv_client_app()
{
    stop();
}

::dsn::error_code simple_kv_client_app::start(int argc, char** argv)
{
    if (argc < 2)
        return ::dsn::ERR_INVALID_PARAMETERS;

    std::vector<rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);

    _meta_server_group.assign_group(dsn_group_build("meta-servers"));
    for (auto& ms : meta_servers)
    {
        dsn_group_add(_meta_server_group.group_handle(), ms.c_addr());
    }

    // argv[1]: e.g., dsn://mycluster/simple-kv.instance0
    _service_addr = url_host_address(argv[1]);
    _simple_kv_client.reset(new simple_kv_client(_service_addr));

    dsn::tasking::enqueue(
            LPC_SIMPLE_KV_TEST,
            this,
            std::bind(&simple_kv_client_app::run, this));

    return ::dsn::ERR_OK;
}

dsn::error_code simple_kv_client_app::stop(bool cleanup)
{
    _simple_kv_client.reset();
    return ::dsn::ERR_OK;
}

void simple_kv_client_app::run()
{
    int id;
    std::string key;
    std::string value;
    int timeout_ms;

    rpc_address receiver;
    dsn::replication::config_type type;
    rpc_address node;

    while (!g_done)
    {
        if (test_case::fast_instance().check_client_write(id, key, value, timeout_ms))
        {
            begin_write(id, key, value, timeout_ms);
            continue;
        }
        if (test_case::fast_instance().check_replica_config(receiver, type, node) )
        {
            send_config_to_meta(receiver, type, node);
            continue;
        }
        if (test_case::fast_instance().check_client_read(id, key, timeout_ms))
        {
            begin_read(id, key, timeout_ms);
            continue;
        }
        test_case::fast_instance().wait_check_client();
    }
}

struct write_context
{
    int id;
    ::dsn::replication::test::kv_pair req;
    int timeout_ms;
};

void simple_kv_client_app::begin_write(int id, const std::string& key, const std::string& value, int timeout_ms)
{
    ddebug("=== on_begin_write:id=%d,key=%s,value=%s,timeout=%d", id, key.c_str(), value.c_str(), timeout_ms);
    std::unique_ptr<write_context> ctx(new write_context());
    ctx->id = id;
    ctx->req.key = key;
    ctx->req.value = value;
    ctx->timeout_ms = timeout_ms;
    auto& req = ctx->req;
    _simple_kv_client->write(
        req,
        [ctx_cap = std::move(ctx)](error_code err, int32_t resp)
        {
            test_case::fast_instance().on_end_write(ctx_cap->id, err, resp);
        },
        std::chrono::milliseconds(timeout_ms)
        );
}


void simple_kv_client_app::send_config_to_meta(const rpc_address& receiver, dsn::replication::config_type type, const rpc_address& node)
{
    dsn_message_t request = dsn_msg_create_request(RPC_CM_MODIFY_REPLICA_CONFIG_COMMAND, 30000);

    ::dsn::marshall(request, g_default_gpid);
    ::dsn::marshall(request, receiver);
    ::dsn::marshall(request, type);
    ::dsn::marshall(request, node);

    dsn_rpc_call_one_way(_meta_server_group.c_addr(), request);
}

struct read_context
{
    int id;
    std::string key;
    int timeout_ms;
};

void simple_kv_client_app::begin_read(int id, const std::string& key, int timeout_ms)
{
    ddebug("=== on_begin_read:id=%d,key=%s,timeout=%d", id, key.c_str(), timeout_ms);
    std::unique_ptr<read_context> ctx(new read_context());
    ctx->id = id;
    ctx->key = key;
    ctx->timeout_ms = timeout_ms;
    _simple_kv_client->read(
        key,
        [ctx_cap = std::move(ctx)](error_code err, std::string&& resp)
        {
            test_case::fast_instance().on_end_read(ctx_cap->id, err, resp);
        },
        std::chrono::milliseconds(timeout_ms));
}


}}}

