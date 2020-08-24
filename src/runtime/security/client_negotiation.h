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

#include "negotiation.h"

namespace dsn {
namespace security {

class client_negotiation : public negotiation
{
public:
    client_negotiation(rpc_session *session);

    void start();

private:
    void handle_response(error_code err, const negotiation_response &&response);
    void list_mechanisms();
    void recv_mechanisms(const negotiation_response &resp);
    void select_mechanism(const std::string &mechanism);
    void send(std::unique_ptr<negotiation_request> request);
    void fail_negotiation();
    void succ_negotiation();
};

} // namespace security
} // namespace dsn
