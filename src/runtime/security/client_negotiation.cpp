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

#include "client_negotiation.h"

#include <boost/algorithm/string/join.hpp>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "fmt/core.h"
#include "negotiation_manager.h"
#include "negotiation_utils.h"
#include "runtime/rpc/network.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/security/negotiation.h"
#include "runtime/security/sasl_wrapper.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

namespace dsn {
namespace security {
extern const std::set<std::string> supported_mechanisms;

client_negotiation::client_negotiation(rpc_session_ptr session) : negotiation(session)
{
    _name = fmt::format("CLIENT_NEGOTIATION(SERVER={})", _session->remote_address().to_string());
}

void client_negotiation::start()
{
    LOG_INFO("{}: start negotiation", _name);
    list_mechanisms();
}

void client_negotiation::list_mechanisms()
{
    _status = negotiation_status::type::SASL_LIST_MECHANISMS;
    send(_status);
}

void client_negotiation::handle_response(error_code err, const negotiation_response &&response)
{
    if (err != ERR_OK) {
        // ERR_HANDLER_NOT_FOUND means server is old version, which doesn't support authentication
        if (ERR_HANDLER_NOT_FOUND == err) {
            LOG_INFO("{}: treat negotiation succeed because server is old version, which doesn't "
                     "support authentication",
                     _name);
            succ_negotiation();
        } else {
            fail_negotiation();
        }
        return;
    }

    // make the negotiation succeed if server doesn't enable auth
    if (negotiation_status::type::SASL_AUTH_DISABLE == response.status) {
        LOG_INFO("{}: treat negotiation succeed as server doesn't enable it", _name);
        succ_negotiation();
        return;
    }

    switch (_status) {
    case negotiation_status::type::SASL_LIST_MECHANISMS:
        on_recv_mechanisms(response);
        break;
    case negotiation_status::type::SASL_SELECT_MECHANISMS:
        on_mechanism_selected(response);
        break;
    case negotiation_status::type::SASL_INITIATE:
    case negotiation_status::type::SASL_CHALLENGE_RESP:
        on_challenge(response);
        break;
    default:
        fail_negotiation();
    }
}

void client_negotiation::on_recv_mechanisms(const negotiation_response &resp)
{
    if (!check_status(resp.status, negotiation_status::type::SASL_LIST_MECHANISMS_RESP)) {
        fail_negotiation();
        return;
    }

    std::string match_mechanism;
    std::vector<std::string> server_support_mechanisms;
    std::string resp_string = resp.msg.to_string();
    utils::split_args(resp_string.c_str(), server_support_mechanisms, ',');

    for (const std::string &server_support_mechanism : server_support_mechanisms) {
        if (supported_mechanisms.find(server_support_mechanism) != supported_mechanisms.end()) {
            match_mechanism = server_support_mechanism;
            break;
        }
    }

    if (match_mechanism.empty()) {
        LOG_WARNING("server only support mechanisms of ({}), can't find expected ({})",
                    boost::join(supported_mechanisms, ","),
                    resp_string);
        fail_negotiation();
        return;
    }

    select_mechanism(match_mechanism);
}

void client_negotiation::on_mechanism_selected(const negotiation_response &resp)
{
    if (!check_status(resp.status, negotiation_status::type::SASL_SELECT_MECHANISMS_RESP)) {
        fail_negotiation();
        return;
    }

    // init client sasl
    auto err_s = _sasl->init();
    if (!err_s.is_ok()) {
        LOG_WARNING("{}: initialize sasl client failed, error = {}, reason = {}",
                    _name,
                    err_s.code(),
                    err_s.description());
        fail_negotiation();
        return;
    }

    // start client sasl, and send `SASL_INITIATE` to `server_negotiation` if everything is ok
    blob start_output;
    err_s = _sasl->start(_selected_mechanism, blob(), start_output);
    if (err_s.is_ok() || ERR_SASL_INCOMPLETE == err_s.code()) {
        _status = negotiation_status::type::SASL_INITIATE;
        send(_status, std::move(start_output));
    } else {
        LOG_WARNING("{}: start sasl client failed, error = {}, reason = {}",
                    _name,
                    err_s.code(),
                    err_s.description());
        fail_negotiation();
    }
}

void client_negotiation::on_challenge(const negotiation_response &challenge)
{
    if (challenge.status == negotiation_status::type::SASL_CHALLENGE) {
        blob response_msg;
        auto err = _sasl->step(challenge.msg, response_msg);
        if (!err.is_ok() && err.code() != ERR_SASL_INCOMPLETE) {
            LOG_WARNING("{}: negotiation failed, reason = {}", _name, err.description());
            fail_negotiation();
            return;
        }

        _status = negotiation_status::type::SASL_CHALLENGE_RESP;
        send(_status, std::move(response_msg));
        return;
    }

    if (challenge.status == negotiation_status::type::SASL_SUCC) {
        succ_negotiation();
        return;
    }

    LOG_WARNING("{}: recv wrong negotiation msg type: {}", _name, enum_to_string(challenge.status));
    fail_negotiation();
}

void client_negotiation::select_mechanism(const std::string &mechanism)
{
    _selected_mechanism = mechanism;
    _status = negotiation_status::type::SASL_SELECT_MECHANISMS;

    send(_status, blob::create_from_bytes(mechanism.data(), mechanism.length()));
}

void client_negotiation::send(negotiation_status::type status, const blob &msg)
{
    auto req = std::make_unique<negotiation_request>();
    req->status = status;
    req->msg = msg;

    negotiation_rpc rpc(std::move(req), RPC_NEGOTIATION);
    rpc.call(_session->remote_address(), nullptr, [rpc](error_code err) mutable {
        negotiation_manager::on_negotiation_response(err, rpc);
    });
}

void client_negotiation::succ_negotiation()
{
    _status = negotiation_status::type::SASL_SUCC;
    _session->set_negotiation_succeed();
    LOG_INFO("{}: negotiation succeed", _name);
}
} // namespace security
} // namespace dsn
