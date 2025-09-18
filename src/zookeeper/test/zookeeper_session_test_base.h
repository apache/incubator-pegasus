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

#include "utils/ports.h"
#include "zookeeper/zookeeper_session.h"

namespace dsn::dist {

class ZookeeperSessionConnector : public testing::Test
{
public:
    ~ZookeeperSessionConnector() override = default;

protected:
    ZookeeperSessionConnector() = default;

    void TearDown() override;

    void test_connect(int expected_zoo_state);

    std::unique_ptr<zookeeper_session> _session;

    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionConnector);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionConnector);
};

class ZookeeperSessionTestBase : public ZookeeperSessionConnector
{
public:
    ~ZookeeperSessionTestBase() override = default;

protected:
    ZookeeperSessionTestBase() = default;

    void SetUp() override;

    void TearDown() override;

    void test_create_node(const std::string &path, const std::string &data, int expected_zerr);

    void test_delete_node(const std::string &path, int expected_zerr);

    void test_delete_node(const std::string &path);

    void test_set_data(const std::string &path, const std::string &data, int expected_zerr);

    void
    test_get_data(const std::string &path, int expected_zerr, const std::string &expected_data);

    void test_get_data(const std::string &path, int expected_zerr);

    void test_exists_node(const std::string &path, int expected_zerr);

    void test_no_node(const std::string &path);

    void test_has_data(const std::string &path, const std::string &data);

private:
    void operate_node(const std::string &path,
                      zookeeper_session::ZOO_OPERATION op_type,
                      const std::string &data,
                      std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
                      int &actual_zerr);

    void operate_node(const std::string &path,
                      zookeeper_session::ZOO_OPERATION op_type,
                      int &actual_zerr);

    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           const std::string &data,
                           std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
                           int expected_zerr);

    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           const std::string &data,
                           int expected_zerr);

    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
                           int expected_zerr);

    void test_operate_node(const std::string &path,
                           zookeeper_session::ZOO_OPERATION op_type,
                           int expected_zerr);

    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionTestBase);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionTestBase);
};

} // namespace dsn::dist
