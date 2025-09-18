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
        // The "this" pointer should be kept here since it is required to delay name lookup
        // while accessing members of the base class that depends on the template parameters.

        // Delete the node if any in case previous tests failed.
        this->test_delete_node(path);

        // Ensure currently the node does not exist.
        this->test_no_node(path);

        // Updating the node will fail since it has not been created.
        this->test_set_data(path, data, ZNONODE);

        // The node has not been created, thus its sub nodes cannot be created.
        this->test_create_node(sub_path, data, ZNONODE);

        // Create the node with some data.
        this->test_create_node(path, data, ZOK);
        this->test_has_data(path, data);

        // Creating the node repeatedly will fail.
        this->test_create_node(path, data, ZNODEEXISTS);

        // Updating the node with another data will succeed since it has been existing.
        const auto another_data = fmt::format("another_{}", data);
        this->test_set_data(path, another_data, ZOK);
        this->test_has_data(path, another_data);

        // Delete the node.
        this->test_delete_node(path, ZOK);
        this->test_no_node(path);

        // Deleting the node repeatedly will fail.
        this->test_delete_node(path, ZNONODE);
    }

private:
    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionTest);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionTest);
};

TYPED_TEST_SUITE_P(ZookeeperSessionTest);

TYPED_TEST_P(ZookeeperSessionTest, OperateNode)
{
    // The node with single-level path.
    static const std::string kPath("/ZookeeperSessionTest");

    // The node with two-level path.
    static const std::string kSubPath(fmt::format("{}/OperateNode", kPath));

    static const std::string kData("hello");

    // Test the node whose path is single-level.
    this->test_node_operations(kPath, kSubPath, kData);

    // Create the node again since next we will test its sub node.
    this->test_create_node(kPath, kData, ZOK);
    this->test_has_data(kPath, kData);

    // Test the sub node whose path is two-level.
    this->test_node_operations(kSubPath, fmt::format("{}/SubNode", kSubPath), "world");
}

REGISTER_TYPED_TEST_SUITE_P(ZookeeperSessionTest, OperateNode);

} // namespace dsn::dist
