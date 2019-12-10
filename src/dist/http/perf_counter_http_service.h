// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/tool-api/http_server.h>

namespace dsn {

class perf_counter_http_service : public http_service
{
public:
    perf_counter_http_service()
    {
        // GET ip:port/perfCounter?name={perf_counter_name}
        register_handler("",
                         std::bind(&perf_counter_http_service::get_perf_counter_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }

    std::string path() const override { return "perfCounter"; }

    void get_perf_counter_handler(const http_request &req, http_response &resp);
};
} // namespace dsn
