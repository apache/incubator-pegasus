#include <vector>
#include <string>
#include <functional>
#include <thread>

#include <boost/lexical_cast.hpp>
#include <gtest/gtest.h>

#include <dsn/service_api_cpp.h>

#include "dist/replication/zookeeper/distributed_lock_service_zookeeper.h"
#include "dist/replication/zookeeper/lock_struct.h"

using namespace dsn;
using namespace dsn::dist;

DEFINE_TASK_CODE(DLOCK_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT)

bool ss_start = false;
bool ss_finish = false;

std::vector<int64_t> q;
int pos = 0;
int64_t result = 0;

class simple_adder_server : public dsn::service_app
{
public:
    simple_adder_server(dsn_gpid gpid) : ::dsn::service_app(gpid) {}

    error_code start(int argc, char **argv)
    {
        ddebug("name: %s, argc=%d", name().c_str(), argc);
        for (int i = 0; i != argc; ++i)
            ddebug("argv: %s", argv[i]);
        while (!ss_start)
            std::this_thread::sleep_for(std::chrono::seconds(1));

        _dlock_service = new distributed_lock_service_zookeeper();
        dassert(_dlock_service->initialize({"/dsn/tests/simple_adder_server"}) == ERR_OK, "");

        distributed_lock_service::lock_options opt = {true, true};
        while (!ss_finish) {
            std::pair<task_ptr, task_ptr> task_pair = _dlock_service->lock(
                "test_lock",
                name(),
                DLOCK_CALLBACK,
                [this](error_code ec, const std::string &name, int version) {
                    EXPECT_TRUE(ERR_OK == ec);
                    EXPECT_TRUE(name == this->name());
                    ddebug("lock: error_code: %s, name: %s, lock version: %d",
                           ec.to_string(),
                           name.c_str(),
                           version);
                },
                DLOCK_CALLBACK,
                [this](error_code, const std::string &, int) { dassert(false, "session expired"); },
                opt);
            task_pair.first->wait();
            for (int i = 0; i < 1000; ++i) {
                if (pos >= q.size()) {
                    ss_finish = true;
                    break;
                }
                result += q[pos++];
            }
            task_ptr unlock_task = _dlock_service->unlock(
                "test_lock", name(), true, DLOCK_CALLBACK, [this](error_code ec) {
                    EXPECT_TRUE(ERR_OK == ec);
                    ddebug("unlock, error code: %s", ec.to_string());
                });
            unlock_task->wait();
            task_pair.second->cancel(false);
        }

        return ERR_OK;
    }

    error_code stop(bool cleanup) override { return ERR_OK; }

private:
    ref_ptr<distributed_lock_service_zookeeper> _dlock_service;
};

TEST(distributed_lock_service_zookeeper, simple_lock_unlock)
{
    pos = 0;
    result = 0;
    ss_start = false;
    ss_finish = false;
    q.clear();

    srand(time(0));
    q.reserve(100000);
    for (int i = 0; i != 100000; ++i) {
        int64_t rand1 = rand() % 10000;
        int64_t rand2 = rand() % 10000;
        q.push_back(rand1 * rand2);
    }

    int64_t expect_reuslt = 0;
    for (int64_t i : q)
        expect_reuslt += i;

    ss_start = true;
    while (!ss_finish)
        std::this_thread::sleep_for(std::chrono::seconds(1));

    ddebug("actual result: %lld, expect_result:%lld", result, expect_reuslt);
    EXPECT_TRUE(result == expect_reuslt);
}

TEST(distributed_lock_service_zookeeper, abnormal_api_call)
{
    ref_ptr<distributed_lock_service_zookeeper> dlock_svc(new distributed_lock_service_zookeeper());
    ASSERT_EQ(ERR_OK, dlock_svc->initialize({"/dsn/tests/simple_adder_server"}));

    std::string lock_id = "test_lock2";
    std::string my_id = "test_myid";
    std::string my_id2 = "test_myid2";

    distributed_lock_service::lock_options opt = {false, true};
    std::pair<task_ptr, task_ptr> cb_pair = dlock_svc->lock(
        lock_id,
        my_id,
        DLOCK_CALLBACK,
        [](error_code ec, const std::string &, int) { ASSERT_TRUE(ERR_OBJECT_NOT_FOUND == ec); },
        DLOCK_CALLBACK,
        nullptr,
        opt);
    ASSERT_TRUE(cb_pair.first != nullptr && cb_pair.second == nullptr);
    cb_pair.first->wait();

    opt.create_if_not_exist = true;
    cb_pair =
        dlock_svc->lock(lock_id,
                        my_id,
                        DLOCK_CALLBACK,
                        [](error_code ec, const std::string &, int) { ASSERT_TRUE(ec == ERR_OK); },
                        DLOCK_CALLBACK,
                        nullptr,
                        opt);
    ASSERT_TRUE(cb_pair.first != nullptr && cb_pair.second != nullptr);
    cb_pair.first->wait();

    // recursive lock
    std::pair<task_ptr, task_ptr> cb_pair2 = dlock_svc->lock(
        lock_id,
        my_id,
        DLOCK_CALLBACK,
        [](error_code ec, const std::string &, int) { ASSERT_TRUE(ec == ERR_RECURSIVE_LOCK); },
        DLOCK_CALLBACK,
        nullptr,
        opt);
    ASSERT_TRUE(cb_pair2.first != nullptr && cb_pair2.second != nullptr);
    cb_pair2.first->wait();
    cb_pair2.second->cancel(false);

    cb_pair.first->wait();
    // try to cancel an locked lock
    task_ptr tsk = dlock_svc->cancel_pending_lock(
        lock_id, my_id, DLOCK_CALLBACK, [](error_code ec, const std::string &, int) {
            ASSERT_TRUE(ec == ERR_INVALID_PARAMETERS);
        });
    tsk->wait();

    // try to cancel an non-exist lock
    tsk = dlock_svc->cancel_pending_lock(
        lock_id, "non-exist-myself", DLOCK_CALLBACK, [](error_code ec, const std::string &, int) {
            ASSERT_TRUE(ec == ERR_OBJECT_NOT_FOUND);
        });
    tsk->wait();

    tsk = dlock_svc->query_lock(
        lock_id, DLOCK_CALLBACK, [my_id](error_code ec, const std::string &name, int) {
            ASSERT_TRUE(ec == ERR_OK);
            ASSERT_TRUE(name == my_id);
        });
    tsk->wait();

    cb_pair2 = dlock_svc->lock(lock_id,
                               my_id2,
                               DLOCK_CALLBACK,
                               [my_id2](error_code ec, const std::string &name, int) {
                                   ASSERT_TRUE(ec == ERR_OK);
                                   ASSERT_TRUE(name == my_id2);
                               },
                               DLOCK_CALLBACK,
                               nullptr,
                               opt);

    bool result = cb_pair2.first->wait(2000);
    ASSERT_FALSE(result);

    tsk = dlock_svc->unlock(
        lock_id, my_id, true, DLOCK_CALLBACK, [](error_code ec) { ASSERT_TRUE(ec == ERR_OK); });

    tsk->wait();
}

void lock_test_init() { dsn::register_app<simple_adder_server>("adder"); }
