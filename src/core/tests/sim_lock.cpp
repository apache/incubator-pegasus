
# include <dsn/service_api_c.h>
# include <dsn/tool_api.h>
# include <dsn/internal/task.h>
# include <dsn/cpp/auto_codes.h>
# include <dsn/cpp/utils.h>
# include <dsn/internal/synchronize.h>
# include <gtest/gtest.h>
# include <thread>
# include "../core/service_engine.h"
# include "../simulator/task_engine.sim.h"
# include "../simulator/scheduler.h"

TEST(tools_simulator, dsn_semaphore)
{
    if(dsn::task::get_current_worker() == nullptr)
        return;
    if(dsn::service_engine::fast_instance().spec().semaphore_factory_name != "dsn::tools::sim_semaphore_provider")
        return;
    dsn_handle_t s = dsn_semaphore_create(2);
    dsn_semaphore_wait(s);
    ASSERT_TRUE(dsn_semaphore_wait_timeout(s, 10));
    ASSERT_FALSE(dsn_semaphore_wait_timeout(s, 0));
    dsn_semaphore_signal(s, 1);
    dsn_semaphore_wait(s);
    dsn_semaphore_destroy(s);
}


TEST(tools_simulator, dsn_lock_nr)
{
    if(dsn::task::get_current_worker() == nullptr)
        return;
    if(dsn::service_engine::fast_instance().spec().lock_nr_factory_name != "dsn::tools::sim_lock_nr_provider")
        return;

    dsn::tools::sim_lock_nr_provider* s = new dsn::tools::sim_lock_nr_provider(nullptr);
    s->lock();
    s->unlock();
    EXPECT_TRUE(s->try_lock());
    s->unlock();
    delete s;
}


TEST(tools_simulator, dsn_lock)
{
    if(dsn::task::get_current_worker() == nullptr)
        return;
    if(dsn::service_engine::fast_instance().spec().lock_factory_name != "dsn::tools::sim_lock_provider")
        return;

    dsn::tools::sim_lock_provider* s = new dsn::tools::sim_lock_provider(nullptr);
    s->lock();
    EXPECT_TRUE(s->try_lock());
    s->unlock();
    s->unlock();
    delete s;
}

namespace dsn{ namespace test{
typedef std::function<void()> system_callback;
}}
TEST(tools_simulator, scheduler)
{
    if(dsn::task::get_current_worker() == nullptr)
        return;
    if(dsn::service_engine::fast_instance().spec().tool != "simulator")
        return;

     dsn::tools::sim_worker_state* s = dsn::tools::scheduler::task_worker_ext::get(dsn::task::get_current_worker());
    dsn::utils::notify_event* evt = new dsn::utils::notify_event();
    dsn::test::system_callback callback = [evt, s](
            void)
    {
        evt->notify();
        s->is_continuation_ready = true;
        return;
    };
    dsn::tools::scheduler::instance().add_system_event(100, callback);
    dsn::tools::scheduler::instance().wait_schedule(true,false);
    evt->wait();
}
