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

#include <functional>
#include <string>

#include "http/http_server.h"
#include "metadata_types.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace replication {
class replica_stub;

class replica_http_service : public http_server_base
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
                         "ip:port/replica/manual_compaction?app_id=<app_id>");
    }

    ~replica_http_service()
    {
        deregister_http_call("replica/duplication");
        deregister_http_call("replica/data_version");
        deregister_http_call("replica/manual_compaction");
    }

    std::string path() const override { return "replica"; }

    void query_duplication_handler(const http_request &req, http_response &resp);
    void query_app_data_version_handler(const http_request &req, http_response &resp);
    void query_manual_compaction_handler(const http_request &req, http_response &resp);

    inline const char *manual_compaction_status_to_string(manual_compaction_status::type status)
    {
        switch (status) {
        case manual_compaction_status::IDLE:
            return "idle";
        case manual_compaction_status::QUEUING:
            return "queuing";
        case manual_compaction_status::RUNNING:
            return "running";
        case manual_compaction_status::FINISHED:
            return "finished";
        default:
            CHECK(false, "invalid status({})", status);
            __builtin_unreachable();
        }
    }

private:
    friend class replica_http_service_test;

    void update_config(const std::string &name) override;

    replica_stub *_stub;
};

} // namespace replication
} // namespace dsn
