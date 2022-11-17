/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <gtest/gtest.h>
#include "meta/meta_state_service.h"
#include <fmt/format.h>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"

#include "meta/meta_state_service_utils.h"

using namespace dsn;
using namespace dsn::replication;

struct meta_state_service_utils_test : ::testing::Test
{
    void SetUp() override
    {
        _svc = utils::factory_store<dist::meta_state_service>::create("meta_state_service_simple",
                                                                      PROVIDER_TYPE_MAIN);

        error_code err = _svc->initialize({});
        ASSERT_EQ(err, ERR_OK);

        _storage = new mss::meta_storage(_svc, &_tracker);
    }

    void TearDown() override
    {
        delete _svc;
        delete _storage;
    }

protected:
    dist::meta_state_service *_svc;
    mss::meta_storage *_storage;
    task_tracker _tracker;
};

TEST_F(meta_state_service_utils_test, create_recursively)
{
    _storage->create_node_recursively(
        std::queue<std::string>({"/1", "2", "3", "4"}), dsn::blob("a", 0, 1), [&]() {
            _storage->get_data("/1", [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });

            _storage->get_data("/1/2", [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });

            _storage->get_data("/1/2/3", [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });

            _storage->get_data("/1/2/3/4",
                               [](const blob &val) { ASSERT_EQ(val.to_string(), "a"); });
        });
    _tracker.wait_outstanding_tasks();

    _storage->create_node_recursively(std::queue<std::string>({"/1"}), dsn::blob("a", 0, 1), [&]() {
        _storage->get_data("/1", [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });
    });
    _tracker.wait_outstanding_tasks();

    _storage->delete_node_recursively("/1", []() {});
    _tracker.wait_outstanding_tasks();
}

TEST_F(meta_state_service_utils_test, delete_and_get)
{
    // create and delete
    _storage->create_node(
        "/2", dsn::blob("b", 0, 1), [&]() { _storage->delete_node("/2", []() {}); });
    _tracker.wait_outstanding_tasks();

    // try get
    _storage->get_data("/2", [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });
    _tracker.wait_outstanding_tasks();
}

TEST_F(meta_state_service_utils_test, delete_recursively)
{
    _storage->create_node_recursively(
        std::queue<std::string>({"/1", "2", "3", "4"}), dsn::blob("c", 0, 1), [&]() {
            _storage->set_data("/1", dsn::blob("c", 0, 1), [&]() {
                _storage->get_data("/1", [](const blob &val) { ASSERT_EQ(val.to_string(), "c"); });
            });
        });
    _tracker.wait_outstanding_tasks();

    _storage->delete_node_recursively("/1", [&]() {
        _storage->get_data("/1", [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });
    });
    _tracker.wait_outstanding_tasks();
}

TEST_F(meta_state_service_utils_test, concurrent)
{
    for (int i = 1; i <= 100; i++) {
        binary_writer w;
        w.write(std::to_string(i));

        _storage->create_node(fmt::format("/{}", i), w.get_buffer(), [&]() {});
    }
    _tracker.wait_outstanding_tasks();

    for (int i = 1; i <= 100; i++) {
        _storage->get_data(fmt::format("/{}", i), [i](const blob &val) {
            binary_reader rd(val);

            std::string value_str;
            rd.read(value_str);
            ASSERT_EQ(value_str, std::to_string(i));
        });
    }
    _tracker.wait_outstanding_tasks();

    // ensure everything is cleared
    for (int i = 1; i <= 100; i++) {
        _storage->delete_node(fmt::format("/{}", i), [i, this]() {
            _storage->get_data(fmt::format("/{}", i),
                               [](const blob &val) { ASSERT_EQ(val.data(), nullptr); });
        });
    }
    _tracker.wait_outstanding_tasks();
}

TEST_F(meta_state_service_utils_test, get_children)
{
    _storage->create_node("/1", dsn::blob(), [this]() {
        _storage->create_node("/1/99", dsn::blob(), []() {});
        _storage->create_node("/1/999", dsn::blob(), []() {});
        _storage->create_node("/1/9999", dsn::blob(), []() {});
    });
    _tracker.wait_outstanding_tasks();

    _storage->get_children("/1", [](bool node_exists, const std::vector<std::string> &children) {
        ASSERT_TRUE(node_exists);

        auto children_copy = children;
        std::sort(children_copy.begin(), children_copy.end());
        ASSERT_EQ(children_copy, std::vector<std::string>({"99", "999", "9999"}));
    });
    _tracker.wait_outstanding_tasks();

    _storage->delete_node("/1/99", []() {});
    _storage->delete_node("/1/999", []() {});
    _storage->delete_node("/1/9999", []() {});
    _tracker.wait_outstanding_tasks();

    _storage->get_children("/1", [](bool node_exists, const std::vector<std::string> &children) {
        ASSERT_TRUE(node_exists);
        ASSERT_EQ(children.size(), 0);
    });
    _tracker.wait_outstanding_tasks();

    _storage->delete_node_recursively("/1", []() {});
    _tracker.wait_outstanding_tasks();

    _storage->get_children("/1", [](bool node_exists, const std::vector<std::string> &children) {
        ASSERT_FALSE(node_exists);
        ASSERT_EQ(children.size(), 0);
    });
    _tracker.wait_outstanding_tasks();
}
