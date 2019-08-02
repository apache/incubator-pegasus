// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/http_server.h>

#include "dist/replication/meta_server/meta_http_service.h"
#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/test/meta_test/misc/misc.h"

#include "meta_service_test_app.h"
#include "meta_test_base.h"

namespace dsn {
namespace replication {

class meta_http_service_test : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();
        _mhs = dsn::make_unique<meta_http_service>(_ms.get());
    }

    std::unique_ptr<meta_http_service> _mhs;

    /// === Tests ===

    void test_get_app_from_primary()
    {
        std::string test_app = "test_app";
        create_app(test_app);
        http_request fake_req;
        http_response fake_resp;
        fake_req.query_args.emplace("name", "test_app");
        _mhs->get_app_handler(fake_req, fake_resp);

        ASSERT_EQ(fake_resp.status_code, http_status_code::ok);
        std::string fake_json = R"({"general":{"app_name":")" + test_app + R"(","app_id":"2)" +
                                R"(","partition_count":"8","max_replica_count":"3"}})" + "\n";
        ASSERT_EQ(fake_resp.body, fake_json);
    }
};

TEST_F(meta_http_service_test, get_app_from_primary) { test_get_app_from_primary(); }

} // namespace replication
} // namespace dsn
