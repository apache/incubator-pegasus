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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/gpid.h"
#include "duplication_test_base.h"
#include "duplication_types.h"
#include "gtest/gtest.h"
#include "http/http_server.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/replica_http_service.h"
#include "replica/test/mock_utils.h"

namespace dsn {
namespace replication {

class dup_replica_http_service_test : public duplication_test_base
{
};

INSTANTIATE_TEST_CASE_P(, dup_replica_http_service_test, ::testing::Values(false, true));

TEST_P(dup_replica_http_service_test, query_duplication_handler)
{
    auto pri = stub->add_primary_replica(1, 1);

    // primary confirmed_decree
    duplication_entry ent;
    ent.dupid = 1583306653;
    ent.progress[pri->get_gpid().get_partition_index()] = 0;
    ent.status = duplication_status::DS_PAUSE;
    add_dup(pri, std::make_unique<replica_duplicator>(ent, pri));

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
        R"({"1583306653":{"1.1":{"duplicating":false,"fail_mode":"FAIL_SLOW","not_confirmed_mutations_num":100,"not_duplicated_mutations_num":50}}})",
        resp.body);
}

} // namespace replication
} // namespace dsn
