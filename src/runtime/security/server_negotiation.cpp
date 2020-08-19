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

#include "server_negotiation.h"
#include "negotiation_utils.h"

#include <boost/algorithm/string/join.hpp>
#include <dsn/utility/strings.h>
#include <dsn/dist/fmt_logging.h>

namespace dsn {
namespace security {

server_negotiation::server_negotiation(rpc_session *session) : negotiation(session)
{
    _name = fmt::format("SERVER_NEGOTIATION(CLIENT={})", _session->remote_address().to_string());
}

void server_negotiation::start()
{
    _status = negotiation_status::type::SASL_LIST_MECHANISMS;
    ddebug_f("{}: start negotiation", _name);
}

void server_negotiation::handle_request(negotiation_rpc rpc)
{
    if (_status == negotiation_status::type::SASL_LIST_MECHANISMS) {
        on_list_mechanisms(rpc);
        return;
    }
}

void server_negotiation::on_list_mechanisms(negotiation_rpc rpc)
{
    if (rpc.request().status == negotiation_status::type::SASL_LIST_MECHANISMS) {
        std::string mech_list = boost::join(supported_mechanisms, ",");
        negotiation_response &response = rpc.response();
        _status = response.status = negotiation_status::type::SASL_LIST_MECHANISMS_RESP;
        response.msg = std::move(mech_list);
    } else {
        ddebug_f("{}: got message({}) while expect({})",
                 _name,
                 enum_to_string(rpc.request().status),
                 enum_to_string(negotiation_status::type::SASL_LIST_MECHANISMS));
        fail_negotiation(rpc, "invalid_client_message_status");
    }
    return;
}

void server_negotiation::fail_negotiation(negotiation_rpc rpc, const std::string &reason)
{
    negotiation_response &response = rpc.response();
    _status = response.status = negotiation_status::type::SASL_AUTH_FAIL;
    response.msg = reason;
}

} // namespace security
} // namespace dsn
