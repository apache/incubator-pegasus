#include <meta_state_service_zookeeper.h>
#include <gtest/gtest.h>
#include <boost/lexical_cast.hpp>

using namespace dsn;
using namespace dsn::dist;

DEFINE_TASK_CODE(META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);

TEST(meta_state_service_zookeeper, basics)
{
#define expect_ok  [](error_code ec){EXPECT_TRUE(ec == ERR_OK);}
#define expect_err [](error_code ec){EXPECT_FALSE(ec == ERR_OK);}
    {
        ref_ptr<meta_state_service_zookeeper> service(new meta_state_service_zookeeper());
        error_code ec = service->initialize("");
        ASSERT_TRUE(ec == ERR_OK);
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
    }

    //check replay
    {
        ref_ptr<meta_state_service_zookeeper> service(new meta_state_service_zookeeper());
        service->initialize("");

        service->node_exist("/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
        service->node_exist("/1/1", META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_err)->wait();
        service->delete_node("/1", false, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, expect_ok)->wait();
    }
    //set & get data
    {
        ref_ptr<meta_state_service_zookeeper> service(new meta_state_service_zookeeper());
        service->initialize("");

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
        
        service->delete_node("/1", true, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec){ ddebug("clear"); } )->wait();
    }
#undef expect_ok
#undef expect_err
}

static void recursively_create(
    ref_ptr<meta_state_service_zookeeper> service,
    clientlet* tracker,
    const std::string& root,
    int current_layer,
    error_code ec)
{
    ddebug("%s: root:%s, layer:%d, ec:%s", __PRETTY_FUNCTION__, root.c_str(), current_layer, ec.to_string());
    ASSERT_TRUE(ec==ERR_OK);
    if (current_layer<=0) return;
    for (int i=0; i!=10; ++i) {
        std::string subroot = root+"/"+boost::lexical_cast<std::string>(i);
        service->create_node(
            subroot, 
            META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, 
            std::bind(recursively_create, service, tracker, subroot, current_layer-1, std::placeholders::_1),
            blob(), 
            tracker
        );
    }
}

TEST(meta_state_service_zookeeper, create_delete)
{
    ref_ptr<meta_state_service_zookeeper> service(new meta_state_service_zookeeper());
    service->initialize("");
    clientlet tracker;
    
    service->delete_node("/r", true, META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, [](error_code ec){ ddebug("result: %s", ec.to_string()); } )->wait();
    service->create_node(
        "/r", 
        META_STATE_SERVICE_SIMPLE_TEST_CALLBACK, 
        std::bind(recursively_create, service, &tracker, "/r", 1, std::placeholders::_1), 
        blob(),
        &tracker);
    dsn_task_tracker_wait_all(tracker.tracker());
    ddebug("create delete test finish");
}
