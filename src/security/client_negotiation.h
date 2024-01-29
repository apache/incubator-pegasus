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
#include "runtime/rpc/rpc_message.h"
#include "security_types.h"
#include "utils/blob.h"

namespace dsn {
class error_code;

namespace security {

// client_negotiation negotiates a session on client side.
class client_negotiation : public negotiation
{
public:
    explicit client_negotiation(rpc_session_ptr session);

    void start() override;
    void handle_response(error_code err, const negotiation_response &&response);

private:
    void on_recv_mechanisms(const negotiation_response &resp);
    void on_mechanism_selected(const negotiation_response &resp);
    void on_challenge(const negotiation_response &resp);

    void list_mechanisms();
    void select_mechanism(const std::string &mechanism);
    void send(negotiation_status::type status, const blob &msg = blob());
    void succ_negotiation();

    friend class client_negotiation_test;
};

} // namespace security
} // namespace dsn
