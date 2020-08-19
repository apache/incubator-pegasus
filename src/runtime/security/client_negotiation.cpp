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

#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/utility/smart_pointers.h>

namespace dsn {
namespace security {

client_negotiation::client_negotiation(rpc_session *session) : negotiation(session)
{
    _name = fmt::format("CLIENT_NEGOTIATION(SERVER={})", _session->remote_address().to_string());
}

void client_negotiation::start()
{
    ddebug_f("{}: start negotiation", _name);
    list_mechanisms();
}

void client_negotiation::handle_response(error_code err, const negotiation_response &&response)
{
    // TBD(zlw)
}

void client_negotiation::list_mechanisms()
{
    auto request = dsn::make_unique<negotiation_request>();
    _status = request->status = negotiation_status::type::SASL_LIST_MECHANISMS;
    send(std::move(request));
}

void client_negotiation::send(std::unique_ptr<negotiation_request> request)
{
    negotiation_rpc rpc(std::move(request), RPC_NEGOTIATION);
    rpc.call(_session->remote_address(), nullptr, [this, rpc](error_code err) mutable {
        handle_response(err, std::move(rpc.response()));
    });
}

} // namespace security
} // namespace dsn
