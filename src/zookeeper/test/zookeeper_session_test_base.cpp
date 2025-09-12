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
#include <functional>

#include "gtest/gtest.h"
#include "runtime/service_app.h"
#include "utils/blob.h"
#include "utils/flags.h"
#include "zookeeper_session_test_base.h"

DSN_DECLARE_int32(timeout_ms);

namespace dsn::dist {

void ZookeeperSessionTestBase::SetUp()
{
    _session = std::make_unique<zookeeper_session>(service_app::current_service_app_info());
    _zoo_state = _session->attach(this, [this](int zoo_state) {
        _zoo_state = zoo_state;

        if (_first_call && zoo_state == ZOO_CONNECTED_STATE) {
            _first_call = false;
            _on_attached.notify();
            return;
        }
    });

    ASSERT_TRUE(_session);

    if (_zoo_state != ZOO_CONNECTED_STATE) {
        _on_attached.wait_for(FLAGS_timeout_ms);

        ASSERT_EQ(ZOO_CONNECTED_STATE, _zoo_state);
    }
}

void ZookeeperSessionTestBase::TearDown() { _session->detach(this); }

void ZookeeperSessionTestBase::test_create_node(const std::string &path,
                                                const std::string &data,
                                                int expected_zerr)
{
    int actual_zerr{0};
    utils::notify_event on_completed;

    auto *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_CREATE;
    op->_input._path = path;
    op->_input._value = blob::create_from_bytes(std::string(data));
    op->_callback_function = [&actual_zerr,
                              &on_completed](zookeeper_session::zoo_opcontext *op) mutable {
        actual_zerr = op->_output.error;

        on_completed.notify();
    };

    _session->visit(op);
    on_completed.wait();

    ASSERT_EQ(expected_zerr, actual_zerr) << "the actual zoo error is: " << zerror(actual_zerr);
}

void ZookeeperSessionTestBase::test_get_data(const std::string &path,
                                             int expected_zerr,
                                             const std::string &expected_data)
{
    int actual_zerr{0};
    std::string actual_data;
    utils::notify_event on_completed;

    auto *op = zookeeper_session::create_context();
    op->_optype = zookeeper_session::ZOO_GET;
    op->_input._path = path;
    op->_input._value = blob::create_from_bytes(std::string(actual_data));
    op->_callback_function =
        [&actual_zerr, &actual_data, &on_completed](zookeeper_session::zoo_opcontext *op) mutable {
            actual_zerr = op->_output.error;
            actual_data.assign(op->_output.get_op.value, op->_output.get_op.value_length);

            on_completed.notify();
        };

    _session->visit(op);
    on_completed.wait();

    ASSERT_EQ(expected_zerr, actual_zerr) << "the actual zoo error is: " << zerror(actual_zerr);
    if (expected_zerr != ZOK) {
        return;
    }

    ASSERT_EQ(expected_data, actual_data);
}

} // namespace dsn::dist
