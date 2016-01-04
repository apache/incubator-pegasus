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
    service->initialize(0, nullptr);
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
        service->initialize(0, nullptr);
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

    typedef dsn::dist::meta_state_service::transaction_entries TEntries;
    //transaction op
    {
        //basic
        dsn::binary_writer writer;
        writer.write(0xdeadbeef);
        std::shared_ptr<TEntries> entries = service->new_transaction_entries(5);
        entries->create_node("/2");
        entries->create_node("/2/2");
        entries->create_node("/2/3");
        entries->set_data("/2", writer.get_buffer());
        entries->delete_node("/2/3");

        auto tsk = service->submit_transaction(entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok);
        tsk->wait();
        for (unsigned int i=0; i<5; ++i) {
            EXPECT_TRUE( entries->get_result(i) == ERR_OK );
        }

        //an invalid operation will stop whole transaction
        entries = service->new_transaction_entries(5);
        entries->create_node("/3");
        entries->create_node("/4");
        entries->delete_node("/2"); //delete a non empty dir
        entries->create_node("/5");

        service->submit_transaction(entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
        error_code err[4] = {ERR_OK, ERR_OK, ERR_INVALID_PARAMETERS, ERR_CONSISTENCY};
        for (unsigned int i=0; i<4; ++i)
            EXPECT_EQ(err[i], entries->get_result(i));

        //another invalid transaction
        entries = service->new_transaction_entries(5);
        entries->create_node("/3");
        entries->create_node("/4");
        entries->delete_node("/5"); // delete a non exist dir
        //although this is also invalid, but ignored due to previous one has stop the transaction
        entries->set_data("/5", writer.get_buffer());

        err[2] = ERR_OBJECT_NOT_FOUND;
        service->submit_transaction(entries, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
        for (unsigned int i=0; i<4; ++i)
            EXPECT_EQ(err[i], entries->get_result(i));
    }

    //check replay with transaction
    {
        delete service;
        service = new ::dsn::dist::meta_state_service_simple();
        service->initialize(0, nullptr);

        service->get_children("/2", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec, const std::vector<std::string>& children)
        {
            ASSERT_TRUE( children.size()==1 && children[0]=="2");
        })->wait();

        service->get_data("/2", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec, const blob& value) {
            ASSERT_TRUE(ec == ERR_OK);
            binary_reader reader(value);
            int value;
            reader.read(value);
            ASSERT_TRUE(value==0xdeadbeef);
        })->wait();
    }

#undef expect_ok
#undef expect_err
}
