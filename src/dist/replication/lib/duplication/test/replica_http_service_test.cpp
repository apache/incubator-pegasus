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

    // primary confirmed_decree
    duplication_entry ent;
    ent.dupid = 1583306653;
    ent.progress[pri->get_gpid().get_partition_index()] = 0;
    ent.status = duplication_status::DS_PAUSE;
    add_dup(pri, make_unique<replica_duplicator>(ent, pri));

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

    auto dup = find_dup(pri, ent.dupid);
    dup->update_progress(duplication_progress().set_last_decree(1050).set_confirmed_decree(1000));
    pri->set_last_committed_decree(1100);
    req.query_args["appid"] = "1";
    http_svc.query_duplication_handler(req, resp);
    ASSERT_EQ(resp.status_code, http_status_code::ok);
    ASSERT_EQ(
        resp.body,
        R"({)"
        R"("1583306653":)"
        R"({"1.1":{"duplicating":false,"not_confirmed_mutations_num":100,"not_duplicated_mutations_num":50}})"
        R"(})");
}

} // namespace replication
} // namespace dsn
