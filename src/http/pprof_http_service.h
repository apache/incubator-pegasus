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

#include <atomic>
#include <functional>
#include <string>

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
                         "[seconds=<heap_profile_seconds>]",
                         "Query a sample of live objects and the stack traces that allocated these "
                         "objects (an environment variable TCMALLOC_SAMPLE_PARAMETER should set to "
                         "a positive value, such as 524288), or the current heap profiling "
                         "information if 'seconds' parameter is specified.");
        register_handler("symbol",
                         std::bind(&pprof_http_service::symbol_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "[symbol_address]",
                         "Query the process' symbols. Return the symbol count of the process if "
                         "using GET, return the symbol of the 'symbol_address' if using POST.");
        register_handler("cmdline",
                         std::bind(&pprof_http_service::cmdline_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "Query the process' cmdline.");
        register_handler("growth",
                         std::bind(&pprof_http_service::growth_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "Query the stack traces that caused growth in the address space size.");
        register_handler("profile",
                         std::bind(&pprof_http_service::profile_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "[seconds=<cpu_profile_seconds>]",
                         "Query the CPU profile. 'seconds' is 60 if not specified.");
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
