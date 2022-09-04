/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

#pragma once

#include <memory>

#include <gtest/gtest.h>

namespace dsn {
namespace replication {
class replication_ddl_client;
} // namespace replication
} // namespace dsn

namespace pegasus {
class pegasus_client;

class test_util : public ::testing::Test
{
public:
    test_util();
    virtual ~test_util();

    static void SetUpTestCase();

    void SetUp() override;

protected:
    std::string cluster_name_;
    std::string app_name_;
    pegasus_client *client = nullptr;
    std::shared_ptr<dsn::replication::replication_ddl_client> ddl_client;
};
} // namespace pegasus
