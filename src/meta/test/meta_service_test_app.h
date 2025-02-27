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

#include <gtest/gtest.h>

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "task/task_code.h"
#include "common/gpid.h"
#include "rpc/serialization.h"
#include "rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "rpc/rpc_address.h"
#include "task/async_calls.h"
#include "rpc/rpc_address.h"
#include "task/async_calls.h"
#include "meta_admin_types.h"
#include "partition_split_types.h"
#include "duplication_types.h"
#include "bulk_load_types.h"
#include "backup_types.h"
#include "consensus_types.h"
#include "replica_admin_types.h"
#include "meta/meta_service_app.h"
#include "meta/server_state.h"
#include "meta/meta_service.h"
#include "common/replication.codes.h"

namespace dsn {
namespace replication {

class spin_counter
{
private:
    std::atomic_int _counter;

public:
    spin_counter() { _counter.store(0); }
    void wait()
    {
        while (_counter.load() != 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    void block() { ++_counter; }
    void notify() { --_counter; }
};

struct reply_context
{
    dsn::message_ex *response;
    spin_counter e;
};

inline dsn::message_ex *create_corresponding_receive(dsn::message_ex *request_msg)
{
    return request_msg->copy(true, true);
}

// fake_receiver_meta_service overrides `reply_message` of meta_service
class fake_receiver_meta_service : public dsn::replication::meta_service
{
public:
    fake_receiver_meta_service() : meta_service()
    {
        _access_controller = security::create_meta_access_controller(nullptr);
    }
    virtual ~fake_receiver_meta_service() {}
    virtual void reply_message(dsn::message_ex *request, dsn::message_ex *response) override
    {
        uint64_t ptr;
        dsn::unmarshall(request, ptr);
        reply_context *ctx = reinterpret_cast<reply_context *>(ptr);
        ctx->response = create_corresponding_receive(response);
        ctx->response->add_ref();

        // release the response
        response->add_ref();
        response->release_ref();

        ctx->e.notify();
    }
};

// release the dsn_message who's reference is 0
inline void destroy_message(dsn::message_ex *msg)
{
    msg->add_ref();
    msg->release_ref();
}

class meta_service_test_app : public dsn::service_app
{
public:
    meta_service_test_app(const dsn::service_app_info *info) : service_app(info) {}

public:
    virtual dsn::error_code start(const std::vector<std::string> &args) override;
    virtual dsn::error_code stop(bool /*cleanup*/) { return dsn::ERR_OK; }
    void state_sync_test();
    void update_configuration_test();
    void balancer_validator();
    void balance_config_file();
    void apply_balancer_test();
    void cannot_run_balancer_test();
    void construct_apps_test();

    void json_compacity();

    // test server_state set_app_envs/del_app_envs/clear_app_envs
    static void app_envs_basic_test();

    // test for bug found
    void adjust_dropped_size();

    void call_update_configuration(
        dsn::replication::meta_service *svc,
        std::shared_ptr<dsn::replication::configuration_update_request> &request);
    void call_config_sync(
        dsn::replication::meta_service *svc,
        std::shared_ptr<dsn::replication::configuration_query_by_node_request> &request);

private:
    typedef std::function<bool(const dsn::replication::app_mapper &)> state_validator;
    bool
    wait_state(dsn::replication::server_state *ss, const state_validator &validator, int time = -1);
};

template <typename TRequest, typename RequestHandler>
std::shared_ptr<reply_context>
fake_rpc_call(dsn::task_code rpc_code,
              dsn::task_code server_state_write_code,
              RequestHandler *handle_class,
              void (RequestHandler::*handle)(dsn::message_ex *request),
              const TRequest &data,
              int hash = 0,
              std::chrono::milliseconds delay = std::chrono::milliseconds(0))
{
    dsn::message_ex *msg = dsn::message_ex::create_request(rpc_code);
    dsn::marshall(msg, data);

    std::shared_ptr<reply_context> result = std::make_shared<reply_context>();
    result->e.block();
    uint64_t ptr = reinterpret_cast<uint64_t>(result.get());
    dsn::marshall(msg, ptr);

    dsn::message_ex *received = create_corresponding_receive(msg);
    received->add_ref();
    dsn::tasking::enqueue(
        server_state_write_code, nullptr, std::bind(handle, handle_class, received), hash, delay);

    // release the sending message
    destroy_message(msg);

    return result;
}

#define fake_create_app(state, request_data)                                                       \
    fake_rpc_call(                                                                                 \
        RPC_CM_CREATE_APP, LPC_META_STATE_NORMAL, state, &server_state::create_app, request_data)

#define fake_drop_app(state, request_data)                                                         \
    fake_rpc_call(                                                                                 \
        RPC_CM_DROP_APP, LPC_META_STATE_NORMAL, state, &server_state::drop_app, request_data)

#define fake_recall_app(state, request_data)                                                       \
    fake_rpc_call(                                                                                 \
        RPC_CM_RECALL_APP, LPC_META_STATE_NORMAL, state, &server_state::recall_app, request_data)

#define fake_create_policy(state, request_data)                                                    \
    fake_rpc_call(RPC_CM_ADD_BACKUP_POLICY,                                                        \
                  LPC_DEFAULT_CALLBACK,                                                            \
                  state,                                                                           \
                  &backup_service::add_backup_policy,                                              \
                  request_data)

#define fake_wait_rpc(context, response_data)                                                      \
    do {                                                                                           \
        context->e.wait();                                                                         \
        ::dsn::unmarshall(context->response, response_data);                                       \
        context->response->release_ref();                                                          \
    } while (0)

} // namespace replication
} // namespace dsn
