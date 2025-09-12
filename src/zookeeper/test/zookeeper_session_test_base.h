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
#include <atomic>
#include <memory>

#include "utils/ports.h"
#include "utils/synchronize.h"
#include "zookeeper/zookeeper_session.h"

namespace dsn::dist {

class ZookeeperSessionTestBase : public testing::Test
{
public:
    ~ZookeeperSessionTestBase() override = default;

protected:
    ZookeeperSessionTestBase() = default;

    void SetUp() override;

    void TearDown() override;

    void test_create_node(const std::string &path, const std::string &data, int expected_zerr);

    void
    test_get_data(const std::string &path, int expected_zerr, const std::string &expected_data);

private:
    std::unique_ptr<zookeeper_session> _session;
    std::atomic_int _zoo_state{0};
    std::atomic_bool _first_call{true};
    utils::notify_event _on_attached;

    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionTestBase);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionTestBase);
};

} // namespace dsn::dist
