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

#include <string>

#include "negotiation.h"
#include "rpc/rpc_message.h"

namespace dsn {
class blob;
class error_s;

namespace security {

// server_negotiation negotiates a session on server side.
class server_negotiation : public negotiation
{
public:
    explicit server_negotiation(rpc_session_ptr session);

    void start() override;

    // handle_request handles negotiate_request from the session.
    void handle_request(negotiation_rpc rpc);

private:
    void on_list_mechanisms(negotiation_rpc rpc);
    void on_select_mechanism(negotiation_rpc rpc);
    void on_initiate(negotiation_rpc rpc);
    void on_challenge_resp(negotiation_rpc rpc);

    void do_challenge(negotiation_rpc rpc, error_s err_s, const blob &resp_msg);
    void succ_negotiation(negotiation_rpc rpc, const std::string &user_name);

    friend class server_negotiation_test;
};

} // namespace security
} // namespace dsn
