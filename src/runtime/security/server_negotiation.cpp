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
    switch (_status) {
    case negotiation_status::type::SASL_LIST_MECHANISMS:
        on_list_mechanisms(rpc);
        break;
    case negotiation_status::type::SASL_LIST_MECHANISMS_RESP:
        on_select_mechanism(rpc);
        break;
    case negotiation_status::type::SASL_SELECT_MECHANISMS_RESP:
    case negotiation_status::type::SASL_CHALLENGE:
        // TBD(zlw)
        break;
    default:
        fail_negotiation();
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
        fail_negotiation();
    }
    return;
}

void server_negotiation::on_select_mechanism(negotiation_rpc rpc)
{
    const negotiation_request &request = rpc.request();
    if (request.status == negotiation_status::type::SASL_SELECT_MECHANISMS) {
        _selected_mechanism = request.msg;
        if (supported_mechanisms.find(_selected_mechanism) == supported_mechanisms.end()) {
            std::string error_msg =
                fmt::format("the mechanism of {} is not supported", _selected_mechanism);
            dwarn_f("{}", error_msg);
            fail_negotiation();
            return;
        }

        error_s err_s = do_sasl_server_init();
        if (!err_s.is_ok()) {
            dwarn_f("{}: server initialize sasl failed, error = {}, msg = {}",
                    _name,
                    err_s.code().to_string(),
                    err_s.description());
            fail_negotiation();
            return;
        }

        negotiation_response &response = rpc.response();
        _status = response.status = negotiation_status::type::SASL_SELECT_MECHANISMS_RESP;
    } else {
        dwarn_f("{}: got message({}) while expect({})",
                _name,
                enum_to_string(request.status),
                negotiation_status::type::SASL_SELECT_MECHANISMS);
        fail_negotiation();
        return;
    }
}

error_s server_negotiation::do_sasl_server_init()
{
    // TBD(zlw)
    return error_s::make(ERR_OK);
}

} // namespace security
} // namespace dsn
