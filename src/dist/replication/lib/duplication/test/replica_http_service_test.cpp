// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "dist/replication/lib/replica_http_service.h"
#include "duplication_test_base.h"

namespace dsn {
namespace replication {

class replica_http_service_test : public duplication_test_base
{
};

TEST_F(replica_http_service_test, query_duplication_handler)
{
    auto pri = stub->add_primary_replica(1, 1);
    auto sec = stub->add_non_primary_replica(1, 2);
    sec->as_secondary();

    // primary confirmed_decree
    duplication_entry ent;
    ent.dupid = 1583306653;
    ent.progress[pri->get_gpid().get_partition_index()] = 1000;
    ent.status = duplication_status::DS_PAUSE;
    auto dup = dsn::make_unique<replica_duplicator>(ent, pri);
    add_dup(pri, std::move(dup));

    sec->get_duplication_manager()->update_confirmed_decree_if_secondary(899);

    replica_http_service http_svc(stub.get());

    http_request req;
    http_response resp;
    http_svc.query_duplication_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::bad_request); // no appid

    req.query_args["appid"] = "2";
    http_svc.query_duplication_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::not_found);

    req.query_args["appid"] = "2xx";
    http_svc.query_duplication_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::bad_request);

    req.query_args["appid"] = "1";
    http_svc.query_duplication_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::ok);
    ASSERT_EQ(resp.body,
              R"({)"
              R"("1583306653":)"
              R"({"1.1":{"confirmed_decree":1000,"duplicating":false,"last_decree":1000}},)"
              R"("non-primaries":)"
              R"({"1.2":{"confirmed_decree":899}})"
              R"(})");
}

} // namespace replication
} // namespace dsn
