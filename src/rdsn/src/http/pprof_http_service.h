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

#ifdef DSN_ENABLE_GPERF

#include "http_server.h"

namespace dsn {

class pprof_http_service : public http_service
{
public:
    pprof_http_service()
    {
        register_handler("heap",
                         std::bind(&pprof_http_service::heap_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/pprof/heap");
        register_handler("symbol",
                         std::bind(&pprof_http_service::symbol_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/pprof/symbol");
        register_handler("cmdline",
                         std::bind(&pprof_http_service::cmdline_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/pprof/cmdline");
        register_handler("growth",
                         std::bind(&pprof_http_service::growth_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/pprof/growth");
        register_handler("profile",
                         std::bind(&pprof_http_service::profile_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/pprof/profile");
    }

    std::string path() const override { return "pprof"; }

    void heap_handler(const http_request &req, http_response &resp);

    void symbol_handler(const http_request &req, http_response &resp);

    void cmdline_handler(const http_request &req, http_response &resp);

    void growth_handler(const http_request &req, http_response &resp);

    void profile_handler(const http_request &req, http_response &resp);

private:
    std::atomic_bool _in_pprof_action{false};
};

} // namespace dsn

#endif // DSN_ENABLE_GPERF
