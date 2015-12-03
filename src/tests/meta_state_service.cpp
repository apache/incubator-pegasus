#include <meta_state_service_simple.h>
#include <gtest/gtest.h>
#include <chrono>
#include <thread>

using namespace dsn;
DEFINE_TASK_CODE(META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);
TEST(meta_state_service_simple, basics)
{
    //environment
    auto service = new ::dsn::dist::meta_state_service_simple();
    service->initialize("./");
    //bondary check
#define expect_ok  [](error_code ec){EXPECT_TRUE(ec == ERR_OK);}
#define expect_err [](error_code ec){EXPECT_FALSE(ec == ERR_OK);}
    service->node_exist("/", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
    service->node_exist("", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
    //recursive delete test
    {
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->create_node("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->get_children("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec, const std::vector<std::string>& children)
        {
            dassert(ec == ERR_OK && children.size() == 1 && *children.begin() == "1", "unexpected child");
        });
        service->node_exist("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->delete_node("/1", false, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
        service->delete_node("/1", true, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
    }
    //repeat create test
    {
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
    }
    //check replay
    {
        delete service;
        service = new ::dsn::dist::meta_state_service_simple();
        service->initialize("./");
        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->node_exist("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
        service->delete_node("/1", false, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
    }
    //set & get data
    {
        dsn::binary_writer writer;
        writer.write(0xdeadbeef);
        service->create_node("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok, writer.get_buffer())->wait();
        service->get_data("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec, const dsn::blob& value)
        {
            expect_ok(ec);
            dsn::binary_reader reader(value);
            int read_value;
            reader.read(read_value);
            dassert(read_value == 0xdeadbeef, "get_value != create_value");
        })->wait();
        writer = dsn::binary_writer();
        writer.write(0xbeefdead);
        service->set_data("/1", writer.get_buffer(), META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->get_data("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec, const dsn::blob& value)
        {
            expect_ok(ec);
            dsn::binary_reader reader(value);
            int read_value;
            reader.read(read_value);
            dassert(read_value == 0xbeefdead, "get_value != create_value");
        })->wait();
    }
#undef expect_ok
#undef expect_err
}
