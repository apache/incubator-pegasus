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

#include <gtest/gtest.h>
#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "http/http_method.h"
#include "http/http_server.h"
#include "runtime/app_model.h"
#include "runtime/service_app.h"
#include "utils/error_code.h"
#include "utils/ports.h"

int gtest_flags = 0;
int gtest_ret = 0;

class test_http_service : public dsn::http_server_base
{
public:
    test_http_service()
    {
        register_handler("get",
                         std::bind(&test_http_service::get_handler,
                                   this,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
                         "ip:port/test/get");
    }

    ~test_http_service() = default;

    std::string path() const override { return "test"; }

private:
    void get_handler(const dsn::http_request &req, dsn::http_response &resp)
    {
        if (req.method != dsn::http_method::GET) {
            resp.body = "please use GET method";
            resp.status_code = dsn::http_status_code::bad_request;
            return;
        }

        resp.body = "you are using GET method";
        resp.status_code = dsn::http_status_code::ok;
    }

    DISALLOW_COPY_AND_ASSIGN(test_http_service);
};

class test_service_app : public dsn::service_app
{
public:
    test_service_app(const dsn::service_app_info *info) : dsn::service_app(info)
    {
        dsn::register_http_service(new test_http_service);
        dsn::start_http_server();
    }

    dsn::error_code start(const std::vector<std::string> &args)
    {
        gtest_ret = RUN_ALL_TESTS();
        gtest_flags = 1;
        return dsn::ERR_OK;
    }
};

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);

    // register all possible services
    dsn::service_app::register_factory<test_service_app>("test");

    dsn_run_config("config-test.ini", false);
    while (gtest_flags == 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

#ifndef ENABLE_GCOV
    dsn_exit(gtest_ret);
#endif
    return gtest_ret;
}
