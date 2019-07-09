// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/http_server.h>
#include <string>

namespace dsn {

class root_http_service : public http_service
{
public:
    root_http_service()
    {
        // url: ip:port/
        register_handler("",
                         std::bind(&root_http_service::default_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2));
    }

    std::string path() const override { return ""; }

    void default_handler(const http_request &req, http_response &resp)
    {
        resp.body = "hello world";
        resp.status_code = http_status_code::ok;
    }
};

} // namespace dsn
