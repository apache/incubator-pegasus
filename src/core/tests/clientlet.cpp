#include <dsn/internal/aio_provider.h>
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/clientlet.h>
#include <functional>
#include "test_utils.h"

DEFINE_TASK_CODE(LPC_TEST_CLIENTLET, TASK_PRIORITY_COMMON, THREAD_POOL_TEST_SERVER)
using namespace dsn;

int global_value;
class test_clientlet: public clientlet {
public:
    std::string str;
    int number;

public:
    test_clientlet(): clientlet(), str("before called"), number(0) {
        global_value = 0;
    }
    void callback_function1()
    {
        check_hashed_access();
        str = "after called";
        ++global_value;
    }

    void callback_function2() {
        check_hashed_access();
        number = 0;
        for (int i=0; i<1000; ++i)
            number+=i;
        ++global_value;
    }

    void callback_function3() { ++global_value; }
    void on_rpc_reply1(error_code ec, std::shared_ptr<std::string>& req, std::shared_ptr<std::string>& resp)
    {
        if (ERR_OK == ec)
            EXPECT_TRUE(req->substr(5) == *resp);
    }

    void on_rpc_reply2(error_code ec, const std::string& response, void* context) {
        EXPECT_TRUE(ec == ERR_OK);
    }
};

TEST(dev_cpp, clientlet_task)
{
    /* normal lpc*/
    test_clientlet *cl = new test_clientlet();
    task_ptr t = tasking::enqueue(LPC_TEST_CLIENTLET, cl, &test_clientlet::callback_function1);
    EXPECT_TRUE(t != nullptr);
    bool result = t->wait();
    EXPECT_TRUE(result==true);
    EXPECT_TRUE(cl->str == "after called");
    delete cl;

    /* task tracking */
    cl = new test_clientlet();
    std::vector<task_ptr> test_tasks;

    t = tasking::enqueue(LPC_TEST_CLIENTLET, cl, &test_clientlet::callback_function1, 0, 30000);
    test_tasks.push_back(t);
    t = tasking::enqueue(LPC_TEST_CLIENTLET, cl, &test_clientlet::callback_function2, 0, 30000);
    test_tasks.push_back(t);
    t = tasking::enqueue(LPC_TEST_CLIENTLET, cl, &test_clientlet::callback_function3, 0, 30000, 20000);
    test_tasks.push_back(t);

    delete cl;
    for (unsigned int i=0; i!=test_tasks.size(); ++i)
        EXPECT_FALSE( test_tasks[i]->cancel(true) );
}

TEST(dev_cpp, clientlet_rpc)
{
    rpc_address addr("localhost", 20101);
    rpc_address addr2("localhost", TEST_PORT_END);
    rpc_address addr3("localhost", 32767);

    test_clientlet* cl = new test_clientlet();
    rpc::call_one_way_typed(addr, RPC_TEST_STRING_COMMAND, std::string("expect_no_reply"), 0);
    std::vector< task_ptr > task_vec;
    const char* command = "echo hello world";

    std::shared_ptr<std::string> str_command(new std::string(command));
    std::function<void (error_code, std::shared_ptr<std::string>&, std::shared_ptr<std::string>&)> call =
            std::bind(&test_clientlet::on_rpc_reply1, cl,
                                              std::placeholders::_1,
                                              std::placeholders::_2,
                                              std::placeholders::_3);
    auto t = rpc::call_typed(addr3, RPC_TEST_STRING_COMMAND, str_command,
                    cl, call,
                    0, 0, 0);
    task_vec.push_back(t);

    char* buf = new char[32];
    memset(buf, 0, 32);
    strcpy(buf, "hello world");

    std::function<void (error_code, const std::string&, void*)> call2 =
            std::bind(&test_clientlet::on_rpc_reply2, cl,
                                              std::placeholders::_1,
                                              std::placeholders::_2,
                                              std::placeholders::_3);

    t = rpc::call_typed(addr2, RPC_TEST_STRING_COMMAND, std::string(command),
                    cl, call2,
                    buf,
                    0, 0, 0);
    task_vec.push_back(t);

    t = rpc::call_typed(addr2, RPC_TEST_STRING_COMMAND, std::string(command),
                    cl, &test_clientlet::on_rpc_reply2, buf,
                    0, 0, 0);
    task_vec.push_back(t);
    for (int i=0; i!=task_vec.size(); ++i)
        task_vec[i]->wait();
}
