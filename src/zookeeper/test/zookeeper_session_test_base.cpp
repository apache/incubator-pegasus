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

#include <zookeeper/zookeeper.h>
#include <zookeeper/zookeeper.jute.h>
#include <algorithm>
#include <atomic>
#include <iostream>
#include <iterator>
#include <memory>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "runtime/service_app.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/flags.h"
#include "utils/synchronize.h"
#include "zookeeper_session_test_base.h"

DSN_DECLARE_int32(timeout_ms);

namespace dsn::dist {

void ZookeeperSessionConnector::TearDown() { _session->detach(this); }

void ZookeeperSessionConnector::test_connect(int expected_zoo_state)
{
    _session = std::make_unique<zookeeper_session>(service_app::current_service_app_info());

    std::atomic_int actual_zoo_state{0};
    std::atomic_bool first_call{true};
    utils::notify_event on_attached;

    const int zoo_state = _session->attach(
        this,
        [expected_zoo_state, &actual_zoo_state, &first_call, &on_attached](int zoo_state) mutable {
            std::cout << "[on_zoo_session_event] zoo_state = " << zoo_state << std::endl;

            actual_zoo_state = zoo_state;

            if (first_call && zoo_state == expected_zoo_state) {
                first_call = false;
                on_attached.notify();
                return;
            }
        });

    ASSERT_TRUE(_session);

    if (zoo_state != expected_zoo_state) {
        on_attached.wait_for(FLAGS_timeout_ms);

        // Callback for attach() may not be called while the state is changed.
        if (actual_zoo_state != 0) {
            ASSERT_EQ(expected_zoo_state, actual_zoo_state);
        }

        ASSERT_EQ(expected_zoo_state, _session->session_state());
    }
}

void ZookeeperSessionTestBase::SetUp() { test_connect(ZOO_CONNECTED_STATE); }

void ZookeeperSessionTestBase::TearDown() { ZookeeperSessionConnector::TearDown(); }

void ZookeeperSessionTestBase::operate_node(
    const std::string &path,
    zookeeper_session::ZOO_OPERATION op_type,
    const std::string &data,
    std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
    int &zerr)
{
    utils::notify_event on_completed;

    auto *op = zookeeper_session::create_context();
    op->_optype = op_type;
    op->_input._path = std::make_shared<std::string>(path);
    op->_input._value = blob::create_from_bytes(std::string(data));
    op->_callback_function =
        [&zerr, &callback, &on_completed](zookeeper_session::zoo_opcontext *op) mutable {
            zerr = op->_output.error;

            if (callback) {
                callback(op);
            }

            on_completed.notify();
        };

    _session->visit(op);
    on_completed.wait();
}

void ZookeeperSessionTestBase::operate_node(const std::string &path,
                                            zookeeper_session::ZOO_OPERATION op_type,
                                            int &zerr)
{
    operate_node(path, op_type, "", nullptr, zerr);
}

void ZookeeperSessionTestBase::test_operate_node(
    const std::string &path,
    zookeeper_session::ZOO_OPERATION op_type,
    const std::string &data,
    std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
    int expected_zerr)
{
    int actual_zerr{0};
    operate_node(path, op_type, data, std::move(callback), actual_zerr);

    ASSERT_EQ(expected_zerr, actual_zerr)
        << "expected_zerr = \"" << zerror(expected_zerr) << "\", actual_zerr = \""
        << zerror(actual_zerr) << "\", path = " << path << ", op_type = " << op_type
        << ", data = \"" << data << "\"";
}

void ZookeeperSessionTestBase::test_operate_node(const std::string &path,
                                                 zookeeper_session::ZOO_OPERATION op_type,
                                                 const std::string &data,
                                                 int expected_zerr)
{
    test_operate_node(path, op_type, data, nullptr, expected_zerr);
}

void ZookeeperSessionTestBase::test_operate_node(
    const std::string &path,
    zookeeper_session::ZOO_OPERATION op_type,
    std::function<void(zookeeper_session::zoo_opcontext *)> &&callback,
    int expected_zerr)
{
    test_operate_node(path, op_type, "", std::move(callback), expected_zerr);
}

void ZookeeperSessionTestBase::test_operate_node(const std::string &path,
                                                 zookeeper_session::ZOO_OPERATION op_type,
                                                 int expected_zerr)
{
    test_operate_node(path, op_type, "", expected_zerr);
}

void ZookeeperSessionTestBase::test_create_node(const std::string &path,
                                                const std::string &data,
                                                int expected_zerr)
{
    test_operate_node(path, zookeeper_session::ZOO_CREATE, data, expected_zerr);
}

void ZookeeperSessionTestBase::test_delete_node(const std::string &path, int expected_zerr)
{
    test_operate_node(path, zookeeper_session::ZOO_DELETE, expected_zerr);
}

void ZookeeperSessionTestBase::test_delete_node(const std::string &path)
{
    using ::testing::AnyOf;
    using ::testing::Eq;

    int actual_zerr{0};
    operate_node(path, zookeeper_session::ZOO_DELETE, actual_zerr);
    ASSERT_THAT(actual_zerr, AnyOf(Eq(ZOK), Eq(ZNONODE)))
        << "expected_zerr = \"" << zerror(ZOK) << "\" or \"" << zerror(ZNONODE)
        << "\", actual_zerr = \"" << zerror(actual_zerr) << "\", path = " << path
        << ", op_type = " << zookeeper_session::ZOO_DELETE;
}

void ZookeeperSessionTestBase::test_set_data(const std::string &path,
                                             const std::string &data,
                                             int expected_zerr)
{
    test_operate_node(path, zookeeper_session::ZOO_SET, data, expected_zerr);
}

void ZookeeperSessionTestBase::test_get_data(const std::string &path,
                                             int expected_zerr,
                                             const std::string &expected_data)
{
    std::string actual_data;
    test_operate_node(
        path,
        zookeeper_session::ZOO_GET,
        [&actual_data](zookeeper_session::zoo_opcontext *op) mutable {
            actual_data.assign(op->_output.get_op.value, op->_output.get_op.value_length);
        },
        expected_zerr);

    if (expected_zerr != ZOK) {
        return;
    }

    ASSERT_EQ(expected_data, actual_data);
}

void ZookeeperSessionTestBase::test_get_data(const std::string &path, int expected_zerr)
{
    test_get_data(path, expected_zerr, "");
}

void ZookeeperSessionTestBase::test_exists_node(const std::string &path, int expected_zerr)
{
    test_operate_node(path, zookeeper_session::ZOO_EXISTS, expected_zerr);
}

void ZookeeperSessionTestBase::get_sub_nodes(const std::string &path,
                                             int &zerr,
                                             std::vector<std::string> &sub_nodes)
{
    utils::notify_event on_completed;

    auto *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_GETCHILDREN;
    op->_input._path = std::make_shared<std::string>(path);
    op->_callback_function =
        [&zerr, &sub_nodes, &on_completed](zookeeper_session::zoo_opcontext *op) mutable {
            zerr = op->_output.error;

            const auto notifier = defer([&on_completed]() { on_completed.notify(); });

            if (zerr != ZOK) {
                return;
            }

            const String_vector *strings = op->_output.getchildren_op.strings;

            sub_nodes.clear();
            sub_nodes.reserve(strings->count);
            std::transform(strings->data,
                           strings->data + strings->count,
                           std::back_inserter(sub_nodes),
                           [](const char *str) { return std::string(str); });
        };

    _session->visit(op);
    on_completed.wait();
}

void ZookeeperSessionTestBase::test_get_sub_nodes(const std::string &path,
                                                  int expected_zerr,
                                                  std::vector<std::string> &&expected_sub_nodes)
{
    int actual_zerr{0};
    std::vector<std::string> actual_sub_nodes;
    get_sub_nodes(path, actual_zerr, actual_sub_nodes);

    ASSERT_EQ(expected_zerr, actual_zerr)
        << "expected_zerr = \"" << zerror(expected_zerr) << "\", actual_zerr = \""
        << zerror(actual_zerr) << "\", path = " << path;

    std::sort(expected_sub_nodes.begin(), expected_sub_nodes.end());
    std::sort(actual_sub_nodes.begin(), actual_sub_nodes.end());
    ASSERT_EQ(expected_sub_nodes, actual_sub_nodes);
}

void ZookeeperSessionTestBase::test_no_node(const std::string &path)
{
    test_exists_node(path, ZNONODE);
    test_get_data(path, ZNONODE);
    test_get_sub_nodes(path, ZNONODE, std::vector<std::string>());
}

void ZookeeperSessionTestBase::test_has_data(const std::string &path, const std::string &data)
{
    test_exists_node(path, ZOK);
    test_get_data(path, ZOK, data);
}

} // namespace dsn::dist
