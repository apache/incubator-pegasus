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

#include "meta/meta_state_service.h"

#include <boost/lexical_cast.hpp>

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

#include "meta/meta_state_service_simple.h"
#include "meta/meta_state_service_zookeeper.h"
#include "utils/fmt_logging.h"

using namespace dsn;
using namespace dsn::dist;

DEFINE_TASK_CODE(META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);

typedef std::function<meta_state_service *()> service_creator_func;
typedef std::function<void(meta_state_service *)> service_deleter_func;

#define expect_ok [](error_code ec) { EXPECT_TRUE(ec == ERR_OK); }
#define expect_err [](error_code ec) { EXPECT_FALSE(ec == ERR_OK); }

void provider_basic_test(const service_creator_func &service_creator,
                         const service_deleter_func &service_deleter)
{
    // environment
    auto service = service_creator();

    // bondary check
    service->node_exist("/", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
    service->node_exist("", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
    // recursive delete test
    {
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->create_node("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->get_children("/1",
                              META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                              [](error_code ec, const std::vector<std::string> &children) {
                                  CHECK(ec == ERR_OK && children.size() == 1 &&
                                            *children.begin() == "1",
                                        "unexpected child");
                              });
        service->node_exist("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->delete_node("/1", false, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)
            ->wait();
        service->delete_node("/1", true, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)
            ->wait();
        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
    }
    // repeat create test
    {
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
    }
    // check replay
    {
        service_deleter(service);
        service = service_creator();
        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->node_exist("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
        service->delete_node("/1", false, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)
            ->wait();
    }
    // set & get data
    {
        dsn::binary_writer writer;
        writer.write(0xdeadbeef);
        service
            ->create_node(
                "/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok, writer.get_buffer())
            ->wait();
        service
            ->get_data("/1",
                       META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                       [](error_code ec, const dsn::blob &value) {
                           expect_ok(ec);
                           dsn::binary_reader reader(value);
                           int read_value = 0;
                           reader.read(read_value);
                           CHECK_EQ(read_value, 0xdeadbeef);
                       })
            ->wait();
        writer = dsn::binary_writer();
        writer.write(0xbeefdead);
        service
            ->set_data(
                "/1", writer.get_buffer(), META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)
            ->wait();
        service
            ->get_data("/1",
                       META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                       [](error_code ec, const dsn::blob &value) {
                           expect_ok(ec);
                           dsn::binary_reader reader(value);
                           int read_value = 0;
                           reader.read(read_value);
                           CHECK_EQ(read_value, 0xbeefdead);
                       })
            ->wait();
    }
    // clean the node created in previos code-block, to support test in next round
    {
        service->delete_node("/1", false, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)
            ->wait();
    }

    typedef dsn::dist::meta_state_service::transaction_entries TEntries;
    // transaction op
    {
        // basic
        dsn::binary_writer writer;
        writer.write(0xdeadbeef);
        std::shared_ptr<TEntries> entries = service->new_transaction_entries(5);
        entries->create_node("/2");
        entries->create_node("/2/2");
        entries->create_node("/2/3");
        entries->set_data("/2", writer.get_buffer());
        entries->delete_node("/2/3");

        auto tsk = service->submit_transaction(
            entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok);
        tsk->wait();
        for (unsigned int i = 0; i < 5; ++i) {
            EXPECT_TRUE(entries->get_result(i) == ERR_OK);
        }

        // an invalid operation will stop whole transaction
        entries = service->new_transaction_entries(5);
        entries->create_node("/3");
        entries->create_node("/4");
        entries->delete_node("/2"); // delete a non empty dir
        entries->create_node("/5");

        service->submit_transaction(entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)
            ->wait();
        error_code err[4] = {ERR_OK, ERR_OK, ERR_INVALID_PARAMETERS, ERR_INCONSISTENT_STATE};
        for (unsigned int i = 0; i < 4; ++i)
            EXPECT_EQ(err[i], entries->get_result(i));

        // another invalid transaction
        entries = service->new_transaction_entries(5);
        entries->create_node("/3");
        entries->create_node("/4");
        entries->delete_node("/5"); // delete a non exist dir
        // although this is also invalid, but ignored due to previous one has stop the transaction
        entries->set_data("/5", writer.get_buffer());

        err[2] = ERR_OBJECT_NOT_FOUND;
        service->submit_transaction(entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)
            ->wait();
        for (unsigned int i = 0; i < 4; ++i)
            EXPECT_EQ(err[i], entries->get_result(i));
    }

    // check replay with transaction
    {
        service_deleter(service);
        service = service_creator();

        service
            ->get_children("/2",
                           META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                           [](error_code ec, const std::vector<std::string> &children) {
                               ASSERT_TRUE(children.size() == 1 && children[0] == "2");
                           })
            ->wait();

        service
            ->get_data("/2",
                       META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                       [](error_code ec, const blob &value) {
                           ASSERT_TRUE(ec == ERR_OK);
                           binary_reader reader(value);
                           int content_value;
                           reader.read(content_value);
                           ASSERT_TRUE(content_value == 0xdeadbeef);
                       })
            ->wait();
    }

    // delete the nodes created just now, using transaction delete
    {
        std::shared_ptr<TEntries> entries = service->new_transaction_entries(2);
        entries->delete_node("/2/2");
        entries->delete_node("/2");

        service->submit_transaction(entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)
            ->wait();
        error_code err[2] = {ERR_OK, ERR_OK};

        for (unsigned int i = 0; i < 2; ++i)
            EXPECT_EQ(err[i], entries->get_result(i));
    }

    service_deleter(service);
}

void recursively_create_node_callback(meta_state_service *service,
                                      dsn::task_tracker *tracker,
                                      const std::string &root,
                                      int current_layer,
                                      error_code ec)
{
    ASSERT_TRUE(ec == ERR_OK);
    if (current_layer <= 0)
        return;

    for (int i = 0; i != 10; ++i) {
        std::string subroot = root + "/" + boost::lexical_cast<std::string>(i);
        service->create_node(subroot,
                             META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                             std::bind(recursively_create_node_callback,
                                       service,
                                       tracker,
                                       subroot,
                                       current_layer - 1,
                                       std::placeholders::_1),
                             blob(),
                             tracker);
    }
}

void provider_recursively_create_delete_test(const service_creator_func &creator,
                                             const service_deleter_func &deleter)
{
    meta_state_service *service = creator();
    dsn::task_tracker tracker;

    service
        ->delete_node("/r",
                      true,
                      META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
                      [](error_code ec) { LOG_INFO("result: %s", ec.to_string()); })
        ->wait();
    service->create_node(
        "/r",
        META_STATE_SERVICE_SIMPLE_TEST_CALLBACK,
        std::bind(
            recursively_create_node_callback, service, &tracker, "/r", 1, std::placeholders::_1),
        blob(),
        &tracker);
    tracker.wait_outstanding_tasks();

    std::this_thread::sleep_for(std::chrono::seconds(1));
    deleter(service);
}

#undef expect_ok
#undef expect_err

TEST(meta_state_service, simple)
{
    auto simple_service_creator = [] {
        meta_state_service_simple *svc = new meta_state_service_simple();
        svc->initialize({});
        return svc;
    };
    auto simple_service_deleter = [](meta_state_service *simple_svc) { delete simple_svc; };

    provider_basic_test(simple_service_creator, simple_service_deleter);
    provider_recursively_create_delete_test(simple_service_creator, simple_service_deleter);
}

TEST(meta_state_service, zookeeper)
{
    auto zookeeper_service_creator = [] {
        meta_state_service_zookeeper *svc = new meta_state_service_zookeeper();
        svc->initialize({});
        return svc;
    };
    auto zookeeper_service_deleter = [](meta_state_service *zookeeper_svc) {
        ASSERT_EQ(zookeeper_svc->finalize(), ERR_OK);
    };

    provider_basic_test(zookeeper_service_creator, zookeeper_service_deleter);
    provider_recursively_create_delete_test(zookeeper_service_creator, zookeeper_service_deleter);
}
