// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/http/http_server.h>

#include "replica_stub.h"

namespace dsn {
namespace replication {

class replica_http_service : public http_service
{
public:
    explicit replica_http_service(replica_stub *stub) : _stub(stub)
    {
        register_handler("duplication",
                         std::bind(&replica_http_service::query_duplication_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/replica/duplication?appid=<appid>");
        register_handler("data_version",
                         std::bind(&replica_http_service::query_app_data_version_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/replica/data_version?app_id=<app_id>");
        register_handler("manual_compaction",
                         std::bind(&replica_http_service::query_manual_compaction_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/replica/maual_compaction?app_id=<app_id>");
    }

    std::string path() const override { return "replica"; }

    void query_duplication_handler(const http_request &req, http_response &resp);
    void query_app_data_version_handler(const http_request &req, http_response &resp);
    void query_manual_compaction_handler(const http_request &req, http_response &resp);

private:
    replica_stub *_stub;
};

} // namespace replication
} // namespace dsn
