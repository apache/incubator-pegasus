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

#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include "http/builtin_http_calls.h"
#include "http/http_server.h"
#include "perf_counter/perf_counter.h"
#include "perf_counter/perf_counter_wrapper.h"

namespace dsn {

TEST(perf_counter_http_service_test, get_perf_counter)
{
    struct test_case
    {
        const char *app;
        const char *section;
        const char *name;
        dsn_perf_counter_type_t type;
        const char *description;
    } tests[] = {
        {"replica", "http", "number", COUNTER_TYPE_NUMBER, "number type"},
        {"replica", "http", "volatile", COUNTER_TYPE_VOLATILE_NUMBER, "volatile type"},
        {"replica", "http", "rate", COUNTER_TYPE_RATE, "rate type"},
        {"replica", "http", "percentline", COUNTER_TYPE_NUMBER_PERCENTILES, "percentline type"}};

    for (auto test : tests) {
        // create perf counter
        perf_counter_wrapper counter;
        counter.init_global_counter(test.app, test.section, test.name, test.type, test.description);

        std::string perf_counter_name;
        perf_counter::build_full_name(test.app, test.section, test.name, perf_counter_name);

        // get perf counter info through the http interface
        http_request fake_req;
        http_response fake_resp;
        fake_req.query_args.emplace("name", perf_counter_name);
        get_perf_counter_handler(fake_req, fake_resp);

        // get fake json based on the perf counter info which is getting above
        std::string fake_json;
        if (COUNTER_TYPE_NUMBER_PERCENTILES == test.type) {
            fake_json = R"({"name":")" + perf_counter_name + R"(",)" +
                        R"("p99":"0.00","p999":"0.00",)" +
                        R"("type":")" + dsn_counter_type_to_string(test.type) + R"(",)" +
                        R"("description":")" + test.description + R"("})" + "\n";
        } else {
            fake_json = R"({"name":")" + perf_counter_name + R"(",)" +
                        R"("value":"0.00",)" +
                        R"("type":")" + dsn_counter_type_to_string(test.type) + R"(",)" +
                        R"("description":")" + test.description + R"("})" + "\n";
        }

        ASSERT_EQ(fake_resp.status_code, http_status_code::ok);
        ASSERT_EQ(fake_resp.body, fake_json);
    }
}
} // namespace dsn
