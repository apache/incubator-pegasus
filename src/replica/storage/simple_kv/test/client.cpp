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

#include "client.h"

#include <stdint.h>
#include <chrono>
#include <functional>
#include <utility>

#include "case.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_other_types.h"
#include "replica/storage/simple_kv/simple_kv.client.h"
#include "replica/storage/simple_kv/test/common.h"
#include "rpc/dns_resolver.h"
#include "rpc/group_host_port.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "simple_kv_types.h"
#include "task/async_calls.h"
#include "task/task_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_code.h"

DSN_DECLARE_string(server_list);

namespace dsn {
namespace replication {
namespace test {

using namespace dsn::replication::application;
DEFINE_TASK_CODE(LPC_SIMPLE_KV_TEST, TASK_PRIORITY_COMMON, dsn::THREAD_POOL_DEFAULT)

simple_kv_client_app::simple_kv_client_app(const service_app_info *info)
    : ::dsn::service_app(info), _simple_kv_client(nullptr)
{
}

simple_kv_client_app::~simple_kv_client_app() { stop(); }

::dsn::error_code simple_kv_client_app::start(const std::vector<std::string> &args)
{
    if (args.size() < 2)
        return ::dsn::ERR_INVALID_PARAMETERS;

    std::vector<host_port> meta_servers;
    replica_helper::parse_server_list(FLAGS_server_list, meta_servers);
    _meta_server_group.assign_group("meta_servers");
    for (const auto &hp : meta_servers) {
        LOG_WARNING_IF(!_meta_server_group.group_host_port()->add(hp), "duplicate adress {}", hp);
    }

    _simple_kv_client.reset(
        new application::simple_kv_client("mycluster", meta_servers, "simple_kv.instance0"));

    dsn::tasking::enqueue(
        LPC_SIMPLE_KV_TEST, &_tracker, std::bind(&simple_kv_client_app::run, this));

    return ::dsn::ERR_OK;
}

dsn::error_code simple_kv_client_app::stop(bool cleanup)
{
    _tracker.cancel_outstanding_tasks();
    _simple_kv_client.reset();
    return ::dsn::ERR_OK;
}

void simple_kv_client_app::run()
{
    int id;
    std::string key;
    std::string value;
    int timeout_ms;

    host_port receiver;
    dsn::replication::config_type::type type;
    host_port node;

    while (!g_done) {
        if (test_case::instance().check_client_write(id, key, value, timeout_ms)) {
            begin_write(id, key, value, timeout_ms);
            continue;
        }
        if (test_case::instance().check_replica_config(receiver, type, node)) {
            send_config_to_meta(receiver, type, node);
            continue;
        }
        if (test_case::instance().check_client_read(id, key, timeout_ms)) {
            begin_read(id, key, timeout_ms);
            continue;
        }
        test_case::instance().wait_check_client();
    }
}

struct write_context
{
    int id;
    ::dsn::replication::test::kv_pair req;
    int timeout_ms;
};

void simple_kv_client_app::begin_write(int id,
                                       const std::string &key,
                                       const std::string &value,
                                       int timeout_ms)
{
    LOG_INFO("=== on_begin_write:id={},key={},value={},timeout={}", id, key, value, timeout_ms);
    std::shared_ptr<write_context> ctx(new write_context());
    ctx->id = id;
    ctx->req.key = key;
    ctx->req.value = value;
    ctx->timeout_ms = timeout_ms;
    auto &req = ctx->req;
    _simple_kv_client->write(
        req,
        [ctx](error_code err, int32_t resp) {
            test_case::instance().on_end_write(ctx->id, err, resp);
        },
        std::chrono::milliseconds(timeout_ms));
}

void simple_kv_client_app::send_config_to_meta(const host_port &receiver,
                                               dsn::replication::config_type::type type,
                                               const host_port &node)
{
    dsn::message_ex *req = dsn::message_ex::create_request(RPC_CM_PROPOSE_BALANCER, 30000);

    configuration_balancer_request request;
    request.gpid = g_default_gpid;

    configuration_proposal_action act;
    SET_IP_AND_HOST_PORT_BY_DNS(act, node, node);
    SET_IP_AND_HOST_PORT_BY_DNS(act, target, receiver);
    act.__set_type(type);
    request.action_list.emplace_back(std::move(act));
    request.__set_force(true);

    dsn::marshall(req, request);

    dsn_rpc_call_one_way(dsn::dns_resolver::instance().resolve_address(_meta_server_group), req);
}

struct read_context
{
    int id;
    std::string key;
    int timeout_ms;
};

void simple_kv_client_app::begin_read(int id, const std::string &key, int timeout_ms)
{
    LOG_INFO("=== on_begin_read:id={},key={},timeout={}", id, key, timeout_ms);
    std::shared_ptr<read_context> ctx(new read_context());
    ctx->id = id;
    ctx->key = key;
    ctx->timeout_ms = timeout_ms;
    _simple_kv_client->read(
        key,
        [ctx](error_code err, std::string &&resp) {
            test_case::instance().on_end_read(ctx->id, err, resp);
        },
        std::chrono::milliseconds(timeout_ms));
}
} // namespace test
} // namespace replication
} // namespace dsn
