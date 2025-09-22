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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "utils/ports.h"
#include "zookeeper/zookeeper_session.h"

namespace dsn::dist {

// As the base class for ZookeeperSessionTest, provides APIs used to test connecting to
// the ZooKeeper.
class ZookeeperSessionConnector : public testing::Test
{
public:
    ~ZookeeperSessionConnector() override = default;

protected:
    ZookeeperSessionConnector() = default;

    void TearDown() override;

    // Connect to ZooKeeper and test the state is `expected_zoo_state` after connected.
    void test_connect(int expected_zoo_state);

    std::unique_ptr<zookeeper_session> _session;

    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionConnector);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionConnector);
};

// As the base class for ZookeeperSessionTest, provides APIs used to test performing
// operations on the ZooKeeper.
class ZookeeperSessionTestBase : public ZookeeperSessionConnector
{
public:
    ~ZookeeperSessionTestBase() override = default;

protected:
    ZookeeperSessionTestBase() = default;

    void SetUp() override;

    void TearDown() override;

    // Test creating the node `path` with `data`: the expected zoo error is `expected_zerr`.
    void test_create_node(const std::string &path, const std::string &data, int expected_zerr);

    // Test deleting the node `path`: the expected zoo error is `expected_zerr`.
    void test_delete_node(const std::string &path, int expected_zerr);

    // Delete the node `path` whether it exists or not.
    void test_delete_node(const std::string &path);

    // Test updating the node `path` with `data`: the expected zoo error is `expected_zerr`.
    void test_set_data(const std::string &path, const std::string &data, int expected_zerr);

    // Test getting data from the node `path`: the expected zoo error and the data it holds
    // are `expected_zerr` and `expected_data`.
    void
    test_get_data(const std::string &path, int expected_zerr, const std::string &expected_data);

    // Test getting data from the node `path`: the expected zoo error is `expected_zerr`
    // while the data it holds is ignored.
    void test_get_data(const std::string &path, int expected_zerr);

    // Test whether the node `path` exists.
    void test_exists_node(const std::string &path, int expected_zerr);

    // Test getting the sub nodes from the node `path`: the expected zoo error and the
    // obtained sub nodes are `expected_zerr` and `expected_sub_nodes`.
    void test_get_sub_nodes(const std::string &path,
                            int expected_zerr,
                            std::vector<std::string> &&expected_sub_nodes);

    // Assert that the node `path` does not exist.
    void test_no_node(const std::string &path);

    // Assert that the node `path` holds `data`.
    void test_has_data(const std::string &path, const std::string &data);

private:
    // Perform an operation on a ZooKeeper node synchronously:
    // - path: the path of the ZooKeeper node.
    // - op_type: the operation type.
    // - data: used for ZOO_CREATE and ZOO_SET.
    // - callback: called after the operation was finished (nothing would be called if
    // nullptr).
    // - zerr: zoo error for the operation.
    void operate_node(const std::string &path,
                      zookeeper_session::ZOO_OPERATION op_type,
                      const std::string &data,
                      std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
                      int &zerr);

    // The same as the above, except that `data` is empty and `callback` is nullptr.
    void operate_node(const std::string &path, zookeeper_session::ZOO_OPERATION op_type, int &zerr);

    // Test an operation on a ZooKeeper node:
    // - path: the path of the ZooKeeper node.
    // - op_type: the operation type.
    // - data: used for ZOO_CREATE and ZOO_SET.
    // - callback: called after the operation was finished (nothing would be called if
    // nullptr).
    // - expected_zerr: expected zoo error for the operation.
    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           const std::string &data,
                           std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
                           int expected_zerr);

    // The same as the above, except that `callback` is nullptr.
    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           const std::string &data,
                           int expected_zerr);

    // The same as the above, except that `data` is empty.
    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
                           int expected_zerr);

    // The same as the above, except that `data` is empty and `callback` is nullptr.
    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           int expected_zerr);

    // Get the sub nodes of a ZooKeeper node:
    // - path: the path of the ZooKeeper node.
    // - zerr: zoo error for the operation.
    // - sub_nodes: the obtained sub nodes.
    void get_sub_nodes(const std::string &path, int &zerr, std::vector<std::string> &sub_nodes);

    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionTestBase);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionTestBase);
};

} // namespace dsn::dist
