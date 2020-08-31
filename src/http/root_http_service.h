// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/http/http_server.h>
#include <dsn/utility/output_utils.h>
#include <string>

#include "http_call_registry.h"

namespace dsn {

class root_http_service : public http_service
{
public:
    explicit root_http_service()
    {
        // url: ip:port/
        register_handler("",
                         std::bind(&root_http_service::default_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/");
    }

    std::string path() const override { return ""; }

    void default_handler(const http_request &req, http_response &resp)
    {
        utils::table_printer tp;
        std::ostringstream oss;
        auto calls = http_call_registry::instance().list_all_calls();
        for (const auto &call : calls) {
            tp.add_row_name_and_data(std::string("/") + call->path, call->help);
        }
        tp.output(oss, utils::table_printer::output_format::kJsonCompact);
        resp.body = oss.str();
        resp.status_code = http_status_code::ok;
    }
};

} // namespace dsn
