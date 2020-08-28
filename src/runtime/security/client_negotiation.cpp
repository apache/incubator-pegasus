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
#include "negotiation_utils.h"

#include <boost/algorithm/string/join.hpp>
#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/utility/smart_pointers.h>
#include <dsn/utility/flags.h>

namespace dsn {
namespace security {
DSN_DECLARE_bool(mandatory_auth);
extern const std::set<std::string> supported_mechanisms;

client_negotiation::client_negotiation(rpc_session *session) : negotiation(session)
{
    _name = fmt::format("CLIENT_NEGOTIATION(SERVER={})", _session->remote_address().to_string());
}

void client_negotiation::start()
{
    ddebug_f("{}: start negotiation", _name);
    list_mechanisms();
}

void client_negotiation::list_mechanisms()
{
    auto request = dsn::make_unique<negotiation_request>();
    _status = request->status = negotiation_status::type::SASL_LIST_MECHANISMS;
    send(std::move(request));
}

void client_negotiation::handle_response(error_code err, const negotiation_response &&response)
{
    if (err != ERR_OK) {
        fail_negotiation();
        return;
    }

    // make the negotiation succeed if server doesn't enable auth and the auth is not mandantory
    if (negotiation_status::type::SASL_AUTH_DISABLE == response.status && !FLAGS_mandatory_auth) {
        ddebug_f("{}: treat negotiation succeed as server doesn't enable it", _name);
        succ_negotiation();
        return;
    }

    switch (_status) {
    case negotiation_status::type::SASL_LIST_MECHANISMS:
        on_recv_mechanisms(response);
        break;
    case negotiation_status::type::SASL_SELECT_MECHANISMS:
        // TBD(zlw)
        break;
    case negotiation_status::type::SASL_INITIATE:
    case negotiation_status::type::SASL_CHALLENGE_RESP:
        // TBD(zlw)
        break;
    default:
        fail_negotiation();
    }
}

void client_negotiation::on_recv_mechanisms(const negotiation_response &resp)
{
    if (resp.status != negotiation_status::type::SASL_LIST_MECHANISMS_RESP) {
        dwarn_f("{}: get message({}) while expect({})",
                _name,
                enum_to_string(resp.status),
                enum_to_string(negotiation_status::type::SASL_LIST_MECHANISMS_RESP));
        fail_negotiation();
        return;
    }

    std::string match_mechanism;
    std::vector<std::string> server_support_mechanisms;
    std::string resp_string = resp.msg;
    utils::split_args(resp_string.c_str(), server_support_mechanisms, ',');

    for (const std::string &server_support_mechanism : server_support_mechanisms) {
        if (supported_mechanisms.find(server_support_mechanism) != supported_mechanisms.end()) {
            match_mechanism = server_support_mechanism;
            break;
        }
    }

    if (match_mechanism.empty()) {
        dwarn_f("server only support mechanisms of ({}), can't find expected ({})",
                resp_string,
                boost::join(supported_mechanisms, ","));
        fail_negotiation();
        return;
    }

    select_mechanism(match_mechanism);
}

void client_negotiation::select_mechanism(const std::string &mechanism)
{
    _selected_mechanism = mechanism;

    auto req = dsn::make_unique<negotiation_request>();
    _status = req->status = negotiation_status::type::SASL_SELECT_MECHANISMS;
    req->msg = mechanism;
    send(std::move(req));
}

void client_negotiation::send(std::unique_ptr<negotiation_request> request)
{
    negotiation_rpc rpc(std::move(request), RPC_NEGOTIATION);
    rpc.call(_session->remote_address(), nullptr, [this, rpc](error_code err) mutable {
        handle_response(err, std::move(rpc.response()));
    });
}

void client_negotiation::succ_negotiation()
{
    _status = negotiation_status::type::SASL_SUCC;
    _session->on_success();
}
} // namespace security
} // namespace dsn
