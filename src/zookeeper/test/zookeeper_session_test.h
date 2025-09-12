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

private:
    DISALLOW_COPY_AND_ASSIGN(ZookeeperSessionTest);
    DISALLOW_MOVE_AND_ASSIGN(ZookeeperSessionTest);
};

TYPED_TEST_SUITE_P(ZookeeperSessionTest);

TYPED_TEST_P(ZookeeperSessionTest, CreateAndGet)
{
    // Just create a single-level dir, otherwise we have to create a multi-level dir
    // recursively.
    static const std::string kPath("/ZookeeperSessionTest");
    static const std::string kData("hello");

    // "this" pointer should be kept here since it is required to delay name lookup while
    // accessing members of the base class that depends on the template parameters.
    this->test_create_node(kPath, kData, ZOK);
    this->test_get_data(kPath, ZOK, kData);
}

// TODO(wangdan): add more test cases for `zookeeper_session`.

REGISTER_TYPED_TEST_SUITE_P(ZookeeperSessionTest, CreateAndGet);

} // namespace dsn::dist
