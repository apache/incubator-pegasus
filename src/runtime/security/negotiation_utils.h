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

#include "security_types.h"

namespace dsn {
namespace security {

// TODO(wutao): rename to negotiation_status_to_string
inline const char *enum_to_string(negotiation_status::type s)
{
    switch (s) {
    case negotiation_status::type::SASL_LIST_MECHANISMS:
        return "negotiation_list_mechanisms";
    case negotiation_status::type::SASL_LIST_MECHANISMS_RESP:
        return "negotiation_list_mechanisms_resp";
    case negotiation_status::type::SASL_SELECT_MECHANISMS:
        return "negotiation_select_mechanisms";
    case negotiation_status::type::SASL_SELECT_MECHANISMS_RESP:
        return "negotiation_select_mechanisms_resp";
    case negotiation_status::type::SASL_SUCC:
        return "negotiation_succ";
    case negotiation_status::type::SASL_AUTH_FAIL:
        return "negotiation_auth_fail";
    case negotiation_status::type::SASL_INITIATE:
        return "negotiation_initiate";
    case negotiation_status::type::SASL_CHALLENGE:
        return "negotiation_challenge";
    case negotiation_status::type::SASL_CHALLENGE_RESP:
        return "negotiation_challenge_response";
    case negotiation_status::type::SASL_AUTH_DISABLE:
        return "negotiation_auth_disable";
    case negotiation_status::type::INVALID:
        return "negotiation_invalid";
    default:
        return "negotiation-unknown";
    }
}

DEFINE_TASK_CODE_RPC(RPC_NEGOTIATION, TASK_PRIORITY_COMMON, dsn::THREAD_POOL_DEFAULT)
} // namespace security
} // namespace dsn
