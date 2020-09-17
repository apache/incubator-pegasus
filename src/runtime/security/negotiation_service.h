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

#include "server_negotiation.h"

#include <dsn/cpp/serverlet.h>
#include <dsn/tool-api/zlocks.h>

namespace dsn {
namespace security {
typedef std::unordered_map<rpc_session *, std::unique_ptr<negotiation>> negotiation_map;

class negotiation_service : public serverlet<negotiation_service>,
                            public utils::singleton<negotiation_service>
{
public:
    static void on_rpc_connected(rpc_session *session);
    static void on_rpc_disconnected(rpc_session *session);

    void open_service();

private:
    negotiation_service();
    void on_negotiation_request(negotiation_rpc rpc);
    friend class utils::singleton<negotiation_service>;
    friend class negotiation_service_test;

    static zrwlock_nr _lock; // [
    static negotiation_map _negotiations;
    //]
};

void init_join_point();
} // namespace security
} // namespace dsn
