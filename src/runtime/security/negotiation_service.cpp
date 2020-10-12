// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "negotiation_service.h"
#include "negotiation_utils.h"
#include "server_negotiation.h"

#include <dsn/utility/flags.h>
#include <dsn/tool-api/zlocks.h>
#include <dsn/dist/failure_detector/fd.code.definition.h>

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_auth);
DSN_DECLARE_bool(mandatory_auth);

inline bool is_negotiation_message(dsn::task_code code)
{
    return code == RPC_NEGOTIATION || code == RPC_NEGOTIATION_ACK;
}

inline bool in_white_list(task_code code)
{
    return is_negotiation_message(code) || fd::is_failure_detector_message(code);
}

negotiation_map negotiation_service::_negotiations;
utils::rw_lock_nr negotiation_service::_lock;

negotiation_service::negotiation_service() : serverlet("negotiation_service") {}

void negotiation_service::open_service()
{
    register_rpc_handler_with_rpc_holder(
        RPC_NEGOTIATION, "Negotiation", &negotiation_service::on_negotiation_request);
}

void negotiation_service::on_negotiation_request(negotiation_rpc rpc)
{
    dassert(!rpc.dsn_request()->io_session->is_client(),
            "only server session receive negotiation request");

    // reply SASL_AUTH_DISABLE if auth is not enable
    if (!security::FLAGS_enable_auth) {
        rpc.response().status = negotiation_status::type::SASL_AUTH_DISABLE;
        return;
    }

    server_negotiation *srv_negotiation = nullptr;
    {
        utils::auto_read_lock l(_lock);
        srv_negotiation =
            static_cast<server_negotiation *>(_negotiations[rpc.dsn_request()->io_session].get());
    }

    dassert(srv_negotiation != nullptr,
            "negotiation is null for msg: {}",
            rpc.dsn_request()->rpc_code().to_string());
    srv_negotiation->handle_request(rpc);
}

void negotiation_service::on_rpc_connected(rpc_session *session)
{
    std::unique_ptr<negotiation> nego = security::create_negotiation(session->is_client(), session);
    nego->start();
    {
        utils::auto_write_lock l(_lock);
        _negotiations[session] = std::move(nego);
    }
}

void negotiation_service::on_rpc_disconnected(rpc_session *session)
{
    {
        utils::auto_write_lock l(_lock);
        _negotiations.erase(session);
    }
}

bool negotiation_service::on_rpc_recv_msg(message_ex *msg)
{
    return !FLAGS_mandatory_auth || in_white_list(msg->rpc_code()) ||
           msg->io_session->is_negotiation_succeed();
}

bool negotiation_service::on_rpc_send_msg(message_ex *msg)
{
    // if try_pend_message return true, it means the msg is pended to the resend message queue
    return !FLAGS_mandatory_auth || in_white_list(msg->rpc_code()) ||
           !msg->io_session->try_pend_message(msg);
}

void init_join_point()
{
    rpc_session::on_rpc_session_connected.put_back(negotiation_service::on_rpc_connected,
                                                   "security");
    rpc_session::on_rpc_session_disconnected.put_back(negotiation_service::on_rpc_disconnected,
                                                      "security");
    rpc_session::on_rpc_recv_message.put_native(negotiation_service::on_rpc_recv_msg);
    rpc_session::on_rpc_send_message.put_native(negotiation_service::on_rpc_send_msg);
}
} // namespace security
} // namespace dsn
