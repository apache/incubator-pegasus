// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#ifdef DSN_ENABLE_GPERF

#include <dsn/tool-api/http_server.h>

namespace dsn {

class pprof_http_service : public http_service
{
public:
    pprof_http_service()
    {
        // ip:port/pprof/heap
        register_handler("heap",
                         std::bind(&pprof_http_service::heap_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));

        // ip:port/pprof/symbol
        register_handler("symbol",
                         std::bind(&pprof_http_service::symbol_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));

        // ip:port/pprof/cmdline
        register_handler("cmdline",
                         std::bind(&pprof_http_service::cmdline_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));

        // ip:port/pprof/growth
        register_handler("growth",
                         std::bind(&pprof_http_service::growth_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }

    std::string path() const override { return "pprof"; }

    void heap_handler(const http_request &req, http_response &resp);

    void symbol_handler(const http_request &req, http_response &resp);

    void cmdline_handler(const http_request &req, http_response &resp);

    void growth_handler(const http_request &req, http_response &resp);
};

} // namespace dsn

#endif // DSN_ENABLE_GPERF
