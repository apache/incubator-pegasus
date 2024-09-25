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

#pragma once

#include <memory>
#include <set>
#include <string>
#include <utility>

#include "rpc/network.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_message.h"
#include "sasl_wrapper.h"
#include "security_types.h"
#include "utils/autoref_ptr.h"

namespace dsn {

namespace security {
typedef rpc_holder<negotiation_request, negotiation_response> negotiation_rpc;

class negotiation
{
public:
    explicit negotiation(rpc_session_ptr session)
        : _session(std::move(session)), _status(negotiation_status::type::INVALID)
    {
        _sasl = create_sasl_wrapper(_session->is_client());
    }

    virtual ~negotiation() = 0;

    virtual void start() = 0;
    bool negotiation_succeed() const { return _status == negotiation_status::type::SASL_SUCC; }
    void fail_negotiation();
    // check whether the status is equal to expected_status
    // ret value:
    //   true:  status == expected_status
    //   false: status != expected_status
    bool check_status(negotiation_status::type status, negotiation_status::type expected_status);

protected:
    // TODO(zlw):we can't get string list from cflags now,
    //  so we should get supported mechanisms from config in the later
    static const std::set<std::string> kSupportedMechanisms;

    rpc_session_ptr _session;
    std::string _name;
    negotiation_status::type _status;
    std::string _selected_mechanism;
    std::unique_ptr<sasl_wrapper> _sasl;
};

std::unique_ptr<negotiation> create_negotiation(bool is_client, rpc_session *session);
} // namespace security
} // namespace dsn
