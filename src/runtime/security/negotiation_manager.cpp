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

#include "negotiation_manager.h"
#include "negotiation_utils.h"
#include "server_negotiation.h"
#include "client_negotiation.h"

#include "utils/flags.h"
#include "utils/zlocks.h"
#include "failure_detector/fd.code.definition.h"
#include "utils/fmt_logging.h"
#include "http/http_server.h"

namespace dsn {
namespace security {
DSN_DECLARE_bool(enable_auth);
DSN_DECLARE_bool(mandatory_auth);

inline bool is_negotiation_message(dsn::task_code code)
{
    return code == RPC_NEGOTIATION || code == RPC_NEGOTIATION_ACK;
}

// in_white_list returns if the rpc code can be allowed to bypass negotiation.
inline bool in_white_list(task_code code)
{
    return is_negotiation_message(code) || fd::is_failure_detector_message(code) ||
           is_http_message(code);
}

/*static*/ negotiation_map negotiation_manager::_negotiations;
/*static*/ utils::rw_lock_nr negotiation_manager::_lock;

negotiation_manager::negotiation_manager() : serverlet("negotiation_manager") {}

void negotiation_manager::open_service()
{
    register_rpc_handler_with_rpc_holder(
        RPC_NEGOTIATION, "Negotiation", &negotiation_manager::on_negotiation_request);
}

void negotiation_manager::on_negotiation_request(negotiation_rpc rpc)
{
    CHECK(!rpc.dsn_request()->io_session->is_client(),
          "only server session receives negotiation request");

    // reply SASL_AUTH_DISABLE if auth is not enable
    if (!security::FLAGS_enable_auth) {
        rpc.response().status = negotiation_status::type::SASL_AUTH_DISABLE;
        return;
    }

    std::shared_ptr<negotiation> nego = get_negotiation(rpc);
    if (nullptr != nego) {
        auto srv_negotiation = static_cast<server_negotiation *>(nego.get());
        srv_negotiation->handle_request(rpc);
    }
}

void negotiation_manager::on_negotiation_response(error_code err, negotiation_rpc rpc)
{
    CHECK(rpc.dsn_request()->io_session->is_client(),
          "only client session receives negotiation response");

    std::shared_ptr<negotiation> nego = get_negotiation(rpc);
    if (nullptr != nego) {
        auto cli_negotiation = static_cast<client_negotiation *>(nego.get());
        cli_negotiation->handle_response(err, std::move(rpc.response()));
    }
}

void negotiation_manager::on_rpc_connected(rpc_session *session)
{
    std::shared_ptr<negotiation> nego = security::create_negotiation(session->is_client(), session);
    nego->start();
    {
        utils::auto_write_lock l(_lock);
        _negotiations[session] = std::move(nego);
    }
}

void negotiation_manager::on_rpc_disconnected(rpc_session *session)
{
    {
        utils::auto_write_lock l(_lock);
        _negotiations.erase(session);
    }
}

// `on_rpc_send_msg` and `on_rpc_recv_msg` will be called by both server and client session.
// For server session, it can bypass negotiation if mandatory_auth is false.
// mandatory_auth is a server-side config only, it doesn't have the same effect for
// client session.
bool negotiation_manager::on_rpc_recv_msg(message_ex *msg)
{
    if (!msg->io_session->is_client() && !FLAGS_mandatory_auth) {
        // if this is server_session and mandatory_auth is turned off.
        return true;
    }

    return dsn_likely(msg->io_session->is_negotiation_succeed()) || in_white_list(msg->rpc_code());
}

bool negotiation_manager::on_rpc_send_msg(message_ex *msg)
{
    if (!msg->io_session->is_client() && !FLAGS_mandatory_auth) {
        // if this is server_session and mandatory_auth is turned off.
        return true;
    }

    // if try_pend_message return true, it means the msg is pended to the resend message queue
    return in_white_list(msg->rpc_code()) || !msg->io_session->try_pend_message(msg);
}

std::shared_ptr<negotiation> negotiation_manager::get_negotiation(negotiation_rpc rpc)
{
    utils::auto_read_lock l(_lock);
    auto it = _negotiations.find(rpc.dsn_request()->io_session);
    if (it == _negotiations.end()) {
        LOG_INFO("negotiation was removed for msg: {}, {}",
                 rpc.dsn_request()->rpc_code().to_string(),
                 rpc.remote_address().to_string());
        return nullptr;
    }

    return it->second;
}

void init_join_point()
{
    rpc_session::on_rpc_session_connected.put_back(negotiation_manager::on_rpc_connected,
                                                   "security");
    rpc_session::on_rpc_session_disconnected.put_back(negotiation_manager::on_rpc_disconnected,
                                                      "security");
    rpc_session::on_rpc_recv_message.put_native(negotiation_manager::on_rpc_recv_msg);
    rpc_session::on_rpc_send_message.put_native(negotiation_manager::on_rpc_send_msg);
}
} // namespace security
} // namespace dsn
