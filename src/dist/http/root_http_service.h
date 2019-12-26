// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/tool-api/http_server.h>
#include <dsn/utility/output_utils.h>
#include <string>

namespace dsn {

class root_http_service : public http_service
{
public:
    explicit root_http_service(http_server *server) : _server(server)
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
        auto help_entries = _server->get_help();
        for (const auto &ent : help_entries) {
            tp.add_row_name_and_data(std::string("/") + ent.name + (ent.method.empty() ? "" : "/") +
                                         ent.method,
                                     ent.help);
        }
        tp.output(oss, utils::table_printer::output_format::kJsonCompact);
        resp.body = oss.str();
        resp.status_code = http_status_code::ok;
    }

private:
    http_server *_server;
};

} // namespace dsn
