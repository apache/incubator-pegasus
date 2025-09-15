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

#include <fmt/core.h>
#include <gtest/gtest.h>
#include <zookeeper/zookeeper.h>
#include <atomic>
#include <memory>

#include "runtime/service_app.h"
#include "utils/ports.h"

namespace dsn::dist {

template <typename BaseFixture>
class ZookeeperSessionTest : public BaseFixture
{
public:
    ~ZookeeperSessionTest() override = default;

protected:
    ZookeeperSessionTest() = default;

    void test_node_operations(const std::string &path,
                              const std::string &sub_path,
                              const std::string &data)
    {
        // "this" pointer should be kept here since it is required to delay name lookup while
        // accessing members of the base class that depends on the template parameters.
        this->test_no_node(path);

        this->test_set_data(path, data, ZNONODE);

        this->test_create_node(sub_path, data, ZNONODE);

        this->test_create_node(path, data, ZOK);
        this->test_has_data(path, data);

        this->test_create_node(path, data, ZNODEEXISTS);

        this->test_delete_node(path, ZOK);
        this->test_no_node(path);

        this->test_delete_node(path, ZNONODE);
    }

private:
    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionTest);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionTest);
};

TYPED_TEST_SUITE_P(ZookeeperSessionTest);

TYPED_TEST_P(ZookeeperSessionTest, OperateNode)
{
    static const std::string kPath("/ZookeeperSessionTest");
    static const std::string kSubPath(fmt::format("{}/OperateNode", kPath));
    static const std::string kData("hello");

    this->test_delete_node(kSubPath);
    this->test_delete_node(kPath);

    this->test_node_operations(kPath, kSubPath, kData);

    this->test_create_node(kPath, kData, ZOK);
    this->test_has_data(kPath, kData);

    this->test_node_operations(kSubPath, fmt::format("{}/SubNode", kSubPath), "world");
}

REGISTER_TYPED_TEST_SUITE_P(ZookeeperSessionTest, OperateNode);

} // namespace dsn::dist
