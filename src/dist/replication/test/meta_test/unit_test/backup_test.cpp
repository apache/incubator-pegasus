#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>

#include "dist/replication/meta_server/meta_backup_service.h"
#include "dist/replication/meta_server/meta_service.h"
#include "meta_service_test_app.h"
#include "../misc/misc.h"

using namespace ::dsn::replication;
using namespace dsn;

struct method_record
{
    dsn::utils::notify_event event;
    int count;
    int max_call_count;
    // whether the event will be triggered when count==max_call_count
    bool trigger_beyond;

    method_record() : event(), count(0), max_call_count(1000000), trigger_beyond(true) {}
};

class mock_base
{
public:
    void reset_records() { _records.clear(); }
protected:
    std::map<std::string, method_record> _records;
};

#define MOCK_ADD_RECORD(records, method_name) records[#method_name] = method_record()
#define MOCK_HELPER_FUNCS(method_name)                                                             \
    int &counter_##method_name() { return _records[#method_name].count; }                          \
    dsn::utils::notify_event &notifier_##method_name() { return _records[#method_name].event; }    \
    int maxcall_##method_name() { return _records[#method_name].max_call_count; }                  \
    void set_maxcall_##method_name(int callcount)                                                  \
    {                                                                                              \
        _records[#method_name].max_call_count = callcount;                                         \
    }                                                                                              \
    bool &trigger_beyond_##method_name() { return _records[#method_name].trigger_beyond; }

#define DEFINE_MOCK0(base_class, method_name)                                                      \
    MOCK_HELPER_FUNCS(method_name)                                                                 \
    void method_name()                                                                             \
    {                                                                                              \
        ddebug("%s is called", #method_name);                                                      \
        int &c = counter_##method_name();                                                          \
        ++c;                                                                                       \
        int max_call = maxcall_##method_name();                                                    \
        if (c <= max_call) {                                                                       \
            base_class::method_name();                                                             \
        }                                                                                          \
        if (c > max_call || (c == max_call && !trigger_beyond_##method_name())) {                  \
            notifier_##method_name().notify();                                                     \
        }                                                                                          \
    }

#define DEFINE_MOCK1(base_class, method_name, type1)                                               \
    MOCK_HELPER_FUNCS(method_name)                                                                 \
    void method_name(type1 arg1)                                                                   \
    {                                                                                              \
        ddebug("%s is called", #method_name);                                                      \
        int &c = counter_##method_name();                                                          \
        ++c;                                                                                       \
        int max_call = maxcall_##method_name();                                                    \
        if (c <= max_call) {                                                                       \
            base_class::method_name(arg1);                                                         \
        }                                                                                          \
        if (c > max_call || (c == max_call && !trigger_beyond_##method_name())) {                  \
            notifier_##method_name().notify();                                                     \
        }                                                                                          \
    }

#define DEFINE_MOCK2(base_class, method_name, type1, type2)                                        \
    MOCK_HELPER_FUNCS(method_name)                                                                 \
    void method_name(type1 arg1, type2 arg2)                                                       \
    {                                                                                              \
        ddebug("%s is called", #method_name);                                                      \
        int &c = counter_##method_name();                                                          \
        ++c;                                                                                       \
        int max_call = maxcall_##method_name();                                                    \
        if (c <= max_call) {                                                                       \
            base_class::method_name(arg1, arg2);                                                   \
        }                                                                                          \
        if (c > max_call || (c == max_call && !trigger_beyond_##method_name())) {                  \
            notifier_##method_name().notify();                                                     \
        }                                                                                          \
    }

#define DEFINE_MOCK3(base_class, method_name, type1, type2, type3)                                 \
    MOCK_HELPER_FUNCS(method_name)                                                                 \
    void method_name(type1 arg1, type2 arg2, type3, arg3)                                          \
    {                                                                                              \
        ddebug("%s is called", #method_name);                                                      \
        int &c = counter_##method_name();                                                          \
        ++c;                                                                                       \
        int max_call = maxcall_##method_name();                                                    \
        if (c <= max_call) {                                                                       \
            base_class::method_name(arg1, arg2, arg3);                                             \
        }                                                                                          \
        if (c > max_call || (c == max_call && !trigger_beyond_##method_name())) {                  \
            notifier_##method_name().notify();                                                     \
        }                                                                                          \
    }

class mock_policy : public policy_context, public mock_base
{
public:
    mock_policy(backup_service *bs) : policy_context(bs) {}
    DEFINE_MOCK0(policy_context, issue_new_backup_unlocked)
    DEFINE_MOCK0(policy_context, continue_current_backup_unlocked)
    DEFINE_MOCK1(policy_context, start_backup_app_meta_unlocked, int32_t)
    DEFINE_MOCK1(policy_context, finish_backup_app_unlocked, int32_t)
    DEFINE_MOCK2(policy_context, write_backup_app_finish_flag_unlocked, int32_t, dsn::task_ptr)

    MOCK_HELPER_FUNCS(start)
    void start()
    {
        ++counter_start();
        notifier_start().notify();
    }
};

void check_backup_info_eq(const backup_info &info1, const backup_info &info2)
{
    ASSERT_EQ(info1.app_ids, info2.app_ids);
    ASSERT_EQ(info1.backup_id, info2.backup_id);
    ASSERT_EQ(info1.end_time_ms, info2.end_time_ms);
    ASSERT_EQ(info1.start_time_ms, info2.start_time_ms);
}

class progress_liar : public meta_service
{
public:
    // req is held by callback, we don't need to handle the life-time of it
    virtual void send_request(dsn::message_ex *req,
                              const rpc_address &target,
                              const rpc_response_task_ptr &callback)
    {
        // need to handle life-time manually
        dsn::message_ex *recved_req = create_corresponding_receive(req);

        backup_request b_req;
        dsn::unmarshall(recved_req, b_req);

        backup_response b_resp;
        b_resp.backup_id = b_req.backup_id;
        b_resp.err = dsn::ERR_OK;
        b_resp.pid = b_req.pid;
        b_resp.policy_name = b_req.policy.policy_name;
        b_resp.progress = check_progress(b_req.pid);

        // need to handle life-time manually
        dsn::message_ex *response_for_send = recved_req->create_response();
        dsn::marshall(response_for_send, b_resp);

        // life time is handled by callback
        dsn::message_ex *response_for_receive = create_corresponding_receive(response_for_send);
        callback->enqueue(dsn::ERR_OK, (dsn::message_ex *)response_for_receive);

        destroy_message(recved_req);
        destroy_message(response_for_send);
    }

    int32_t check_progress(const gpid &pid)
    {
        if (progress.find(pid) == progress.end()) {
            progress[pid] = 500;
        } else if (progress[pid] == 500) {
            progress[pid] = 250;
        } else if (progress[pid] == 250) {
            progress[pid] = 1000;
        }

        return progress[pid];
    }

private:
    std::map<gpid, int32_t> progress;
};

static const std::string test_policy_name = "test_policy_name";

void meta_service_test_app::policy_context_test()
{
    const std::string policy_root = "/test";
    const std::string policy_dir = "/test/" + test_policy_name;

    std::shared_ptr<meta_service> s = std::make_shared<progress_liar>();
    dsn::error_code ec = s->remote_storage_initialize();
    ASSERT_EQ(ec, dsn::ERR_OK);
    server_state *state = s->_state.get();
    s->_started = true;
    s->_backup_handler = std::make_shared<backup_service>(s.get(), policy_root, ".", nullptr);
    s->_backup_handler->backup_option().app_dropped_retry_delay_ms = 500_ms;
    s->_backup_handler->backup_option().request_backup_period_ms = 20_ms;
    s->_backup_handler->backup_option().issue_backup_interval_ms = 1000_ms;
    s->_storage
        ->create_node(
            policy_root, dsn::TASK_CODE_EXEC_INLINED, [&ec](dsn::error_code err) { ec = err; })
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, ec);

    mock_policy mp(s->_backup_handler.get());
    policy p;
    p.policy_name = test_policy_name;
    p.is_disable = false;
    p.backup_interval_seconds = 5;
    p.backup_provider_type = "local_service";
    p.start_time = backup_start_time(24, 0);
    p.app_ids = {1, 2, 3, 4, 6};
    p.app_names[1] = "app1";
    p.app_names[2] = "app2";
    p.app_names[3] = "app3";
    p.app_names[4] = "app4";
    p.app_names[6] = "app6";

    mp.set_policy(policy(p));
    s->_storage
        ->create_node(
            policy_dir, dsn::TASK_CODE_EXEC_INLINED, [&ec](dsn::error_code err) { ec = err; })
        ->wait();
    ASSERT_EQ(dsn::ERR_OK, ec);

    int64_t time_before_backup = static_cast<int64_t>(dsn_now_ms());

    {
        // Prepare: backup_history is empty, all apps are deleted.
        // Result: we can't get continue-curr called, issue will be recalled again
        std::cout << "issue a backup, but no app is available" << std::endl;

        {
            zauto_lock l(mp._lock);
            mp.set_maxcall_issue_new_backup_unlocked(2);
            mp.issue_new_backup_unlocked();
        }

        ASSERT_TRUE(mp.notifier_issue_new_backup_unlocked().wait_for(5000));

        {
            zauto_lock l(mp._lock);
            ASSERT_EQ(0, mp.counter_continue_current_backup_unlocked());
            ASSERT_LE(time_before_backup, mp._cur_backup.backup_id);
            ASSERT_EQ(p.app_ids, mp._cur_backup.app_ids);
            ASSERT_NE(0, mp._cur_backup.start_time_ms);
            ASSERT_TRUE(mp._progress.unfinished_partitions_per_app.empty());
            ASSERT_EQ(p.app_ids.size(), mp._progress.unfinished_apps);
            ASSERT_LE(test_policy_name + std::string("@") + std::to_string(time_before_backup),
                      mp._backup_sig);
        }
    }

    {
        // Prepare: backup_history is empty
        //          not all apps are deleted.
        // Result: we can get continue-curr called
        std::cout << "issue a new backup without backup histories" << std::endl;
        dsn::app_info info;
        info.is_stateful = true;
        info.app_id = 3;
        info.app_type = "simple_kv";
        info.max_replica_count = 3;
        info.partition_count = 32;
        info.status = dsn::app_status::AS_AVAILABLE;
        state->_all_apps.emplace(info.app_id, app_state::create(info));

        {
            zauto_lock l(mp._lock);
            mp.reset_records();
            mp.set_maxcall_continue_current_backup_unlocked(0);
            mp.issue_new_backup_unlocked();
        }

        ASSERT_TRUE(mp.notifier_continue_current_backup_unlocked().wait_for(5000));

        {
            zauto_lock l(mp._lock);
            ASSERT_EQ(p.app_ids.size(), mp._progress.unfinished_apps);
            ASSERT_EQ(1, mp._progress.unfinished_partitions_per_app.size());
            ASSERT_EQ(info.app_id, mp._progress.unfinished_partitions_per_app.begin()->first);
            ASSERT_EQ(info.partition_count,
                      mp._progress.unfinished_partitions_per_app.begin()->second);
            ASSERT_EQ(info.partition_count, mp._progress.partition_progress.size());
        }
    }

    {
        // test cases
        // Prepare: backup_history isn't empty,
        //          all apps are unavailable,
        //          we will reach next backup time 500ms later
        // Result: issue called 3 times
        std::cout << "issue a new backup later" << std::endl;

        backup_info info;
        info.app_ids = {1, 2, 3};

        info.start_time_ms = dsn_now_ms() - (p.backup_interval_seconds + 20) * 1000 - 500;
        info.end_time_ms = info.start_time_ms + 10;
        info.backup_id = info.start_time_ms;
        mp.add_backup_history(info);

        info.start_time_ms += 10000;
        info.end_time_ms += 10000;
        info.backup_id = info.start_time_ms;
        mp.add_backup_history(info);

        // the start time for recent backup is 500ms ago
        info.start_time_ms += 10000;
        info.end_time_ms += 10000;
        info.backup_id = info.start_time_ms;
        mp.add_backup_history(info);

        {
            zauto_lock l(mp._lock);
            mp.reset_records();
            // issue by test -> issue by period delay -> issue by dropped retry ->
            // issue by dropped retry
            mp.set_maxcall_issue_new_backup_unlocked(4);
            state->_all_apps[3]->status = dsn::app_status::AS_DROPPED;

            mp.issue_new_backup_unlocked();
        }
        // we mark all apps as dropped, so reissue will be triggered
        ASSERT_TRUE(mp.notifier_issue_new_backup_unlocked().wait_for(20000));

        {
            int64_t start_time_ms_of_sixth_backup =
                info.start_time_ms + p.backup_interval_seconds * 1000;
            zauto_lock l(mp._lock);
            ASSERT_LE(start_time_ms_of_sixth_backup, mp._cur_backup.backup_id);
            ASSERT_EQ(p.app_ids, mp._cur_backup.app_ids);

            // every time intialize backup, the progress will be reset
            ASSERT_TRUE(mp._progress.unfinished_partitions_per_app.empty());
            ASSERT_TRUE(mp._progress.partition_progress.empty());
            ASSERT_EQ(p.app_ids.size(), mp._progress.unfinished_apps);
            ASSERT_LE(test_policy_name + "@" + std::to_string(start_time_ms_of_sixth_backup),
                      mp._backup_sig);
        }
    }

    {
        // test case: continue current backup unlocked
        // Prepare: app 3 is available
        //          clear the backup list
        //          call continue_current_backup_unlocked.
        // Result: app {1, 2, 4, 6} will treat as finished, both finish_backup_app_unlocked
        //         and write_backup_app_finish_flag_unlocked will be called 4 times.
        //         start_backup_app_meta is called for app 3, only called once,
        //         as app 3 won't be finished, so the backup can't finish
        std::cout << "continue backup, only some apps are available " << std::endl;
        {
            zauto_lock l(mp._lock);
            mp._backup_history.clear();
            mp.reset_records();
            mp.set_maxcall_start_backup_app_meta_unlocked(0);

            mp.set_maxcall_finish_backup_app_unlocked(4);
            mp.trigger_beyond_finish_backup_app_unlocked() = false;
            mp.set_maxcall_write_backup_app_finish_flag_unlocked(4);
            mp.trigger_beyond_write_backup_app_finish_flag_unlocked() = false;

            state->_all_apps[3]->status = dsn::app_status::AS_AVAILABLE;
            mp.issue_new_backup_unlocked();
        }

        ASSERT_TRUE(mp.notifier_start_backup_app_meta_unlocked().wait_for(10000));
        ASSERT_TRUE(mp.notifier_finish_backup_app_unlocked().wait_for(10000));
        ASSERT_TRUE(mp.notifier_write_backup_app_finish_flag_unlocked().wait_for(10000));

        {
            zauto_lock l(mp._lock);
            ASSERT_EQ(1, mp.counter_start_backup_app_meta_unlocked());
            ASSERT_EQ(4, mp.counter_finish_backup_app_unlocked());
        }
    }

    {
        // test case: app is dropped when start backup meta
        // Prepare: prepare the current backup, then mark the app as dropped
        // Result: all apps will be marked as finished, new backup will be issued
        std::cout << "app is dropped when start to backup meta" << std::endl;
        app_state *app = state->_all_apps[3].get();

        {
            zauto_lock l(mp._lock);
            mp._backup_history.clear();
            mp.reset_records();

            mp.prepare_current_backup_on_new_unlocked();
            dsn::task_ptr tsk = tasking::create_task(TASK_CODE_EXEC_INLINED, nullptr, []() {});
            mp.sync_backup_to_remote_storage_unlocked(mp._cur_backup, tsk, true);
            tsk->wait();
            mp.set_maxcall_issue_new_backup_unlocked(1);

            ASSERT_EQ(mp._progress.unfinished_apps, p.app_ids.size());
            app->status = dsn::app_status::AS_DROPPED;

            mp.continue_current_backup_unlocked();
        }

        // new backup will be issued 5s later.
        ASSERT_TRUE(mp.notifier_issue_new_backup_unlocked().wait_for(20000));

        {
            zauto_lock l(mp._lock);
            ASSERT_EQ(0, mp._cur_backup.end_time_ms);
            ASSERT_EQ(0, mp._progress.unfinished_apps);
            ASSERT_EQ(app->partition_count, mp._progress.partition_progress.size());

            const backup_info &history = mp._backup_history.begin()->second;
            ASSERT_NE(0, history.start_time_ms);
            ASSERT_GE(history.end_time_ms, history.start_time_ms);

            for (const auto &kv : mp._progress.partition_progress) {
                ASSERT_EQ(kv.first.get_app_id(), app->app_id);
                ASSERT_EQ(kv.second, 1000);
            }
            for (const auto &kv : mp._progress.unfinished_partitions_per_app) {
                ASSERT_EQ(0, kv.second);
            }
        }
    }

    {
        // test_case: a full backup procedure
        // Prepare: issue a new backup
        // Result: a new backup will be issued, and we have a entry on remote storage
        std::cout << "a successful entire backup" << std::endl;
        int64_t cur_start_time_ms = static_cast<int64_t>(dsn_now_ms());
        {
            zauto_lock l(mp._lock);
            std::vector<dsn::rpc_address> node_list;
            generate_node_list(node_list, 3, 3);

            app_state *app = state->_all_apps[3].get();
            app->status = dsn::app_status::AS_AVAILABLE;
            for (partition_configuration &pc : app->partitions) {
                pc.primary = node_list[0];
                pc.secondaries = {node_list[1], node_list[2]};
            }

            mp._backup_history.clear();
            mp.reset_records();

            // issue_in_test -> issued by finish all apps -> a delay for backup interval
            mp.set_maxcall_issue_new_backup_unlocked(2);
            mp.issue_new_backup_unlocked();
        }

        ASSERT_TRUE(mp.notifier_issue_new_backup_unlocked().wait_for(10000));

        {
            zauto_lock l(mp._lock);
            // as new backup is captured and abandoned, so we can check the current backup
            ASSERT_EQ(1, mp._backup_history.size());
            // the first backup's id is 1
            ASSERT_LE(cur_start_time_ms, mp._backup_history.begin()->first);
            const backup_info &history = mp._backup_history.begin()->second;
            ASSERT_NE(0, history.start_time_ms);
            ASSERT_GE(history.end_time_ms, history.start_time_ms);

            // check the progress
            for (const auto &kv : mp._progress.partition_progress) {
                ASSERT_EQ(kv.second, 1000);
            }
            ASSERT_EQ(0, mp._progress.unfinished_apps);
        }
    }

    {
        // test case: add backup_history
        std::cout << "test add backup history" << std::endl;

        mp._backup_history.clear();
        mp._cur_backup.backup_id = 0;
        mp._cur_backup.end_time_ms = 0;

        backup_info bi;
        bi.start_time_ms = 100;
        bi.end_time_ms = 110;
        bi.app_ids = {1, 2, 3};
        bi.backup_id = bi.start_time_ms;
        mp.add_backup_history(bi);

        bi.start_time_ms += 1000;
        bi.end_time_ms += 1000;
        bi.app_ids = {1, 2, 5};
        bi.backup_id = bi.start_time_ms;
        mp.add_backup_history(bi);

        bi.start_time_ms += 1000;
        bi.end_time_ms = 0;
        bi.app_ids = {1, 2, 7};
        bi.backup_id = bi.start_time_ms;

        mp.add_backup_history(bi);

        ASSERT_EQ(bi.backup_id, mp._cur_backup.backup_id);
        ASSERT_EQ(bi.app_ids, mp._cur_backup.app_ids);
        ASSERT_EQ(0, mp._cur_backup.end_time_ms);

        ASSERT_EQ(bi.app_ids.size(), mp._progress.unfinished_apps);
        ASSERT_EQ(2, mp._backup_history.size());

        std::string cur_backup_sig =
            test_policy_name + std::string("@") + std::to_string(bi.backup_id);
        ASSERT_EQ(cur_backup_sig, mp._backup_sig);
    }

    {
        // clear the remote state
        s->_storage
            ->delete_node(policy_root,
                          true,
                          TASK_CODE_EXEC_INLINED,
                          [&ec](dsn::error_code err) { ec = err; },
                          nullptr)
            ->wait();
        ASSERT_EQ(dsn::ERR_OK, ec);
    }

    // test should_start_backup_unlock()
    {
        std::cout << "test should_start_backup_unlock()" << std::endl;
        uint64_t now = dsn_now_ms();
        int32_t hour = 0, min = 0, sec = 0;
        ::dsn::utils::time_ms_to_date_time(now, hour, min, sec);
        while (min == 59) {
            std::this_thread::sleep_for(std::chrono::minutes(1));
            now = dsn_now_ms();
            ::dsn::utils::time_ms_to_date_time(now, hour, min, sec);
        }

        int64_t oneday_sec = 1 * 24 * 60 * 60;
        mp._policy.start_time.hour = hour;
        mp._policy.start_time.minute = 0;
        mp._policy.backup_interval_seconds = oneday_sec; // oneday
        mp._backup_history.clear();

        backup_info info;

        {
            std::cout << "first backup & no limit to start_time" << std::endl;
            mp._policy.start_time.hour = 24;
            ASSERT_TRUE(mp.should_start_backup_unlocked());
        }

        {
            std::cout << "first backup & cur_time.hour == start_time.hour" << std::endl;
            mp._policy.start_time.hour = hour;
            ASSERT_TRUE(mp.should_start_backup_unlocked());
        }

        {
            std::cout << "first backup & cur_time.hour != start_time.hour" << std::endl;
            mp._policy.start_time.hour = hour + 100; // invalid time
            ASSERT_FALSE(mp.should_start_backup_unlocked());
            mp._policy.start_time.hour = (hour + 1) % 24; // valid, but not reach
            ASSERT_FALSE(mp.should_start_backup_unlocked());
            mp._policy.start_time.hour = hour - 1; // time passed(also, include -1)
            ASSERT_FALSE(mp.should_start_backup_unlocked());
        }

        {
            std::cout << "not first backup & recent backup delay 20min to start" << std::endl;
            info.start_time_ms = now - (oneday_sec * 1000) + 20 * 60 * 1000;
            info.end_time_ms = info.start_time_ms + 10;
            mp.add_backup_history(info);
            // if we set start_time to 24:00, then will not start backup
            mp._policy.start_time.hour = 24;
            ASSERT_FALSE(mp.should_start_backup_unlocked());
            // if we set start_time to hour:00, then will start backup, even if the interval <
            // policy.backup_interval
            mp._policy.start_time.hour = hour;
            ASSERT_TRUE(mp.should_start_backup_unlocked());
        }

        {
            std::cout << "not first backup & recent backup start time is equal with start_time"
                      << std::endl;
            mp._policy.start_time.hour = hour;
            mp._backup_history.clear();
            info.start_time_ms = now - (oneday_sec * 1000) - (min * 60 * 1000);
            info.start_time_ms = (info.start_time_ms / 1000) * 1000;
            info.end_time_ms = info.start_time_ms + 10;
            mp.add_backup_history(info);
            ASSERT_TRUE(mp.should_start_backup_unlocked());
        }

        {
            // delay the start_time
            std::cout << "not first backup & delay the start time of policy" << std::endl;
            mp._policy.start_time.hour = hour + 1;
            mp._backup_history.clear();
            // make sure the start time of recent backup is litte than policy's start_time, so we
            // minus more 3min
            info.start_time_ms = now - (oneday_sec * 1000) - 3 * 60 * 1000;
            info.end_time_ms = info.start_time_ms + 10;
            mp.add_backup_history(info);
            if (mp._policy.start_time.hour == 24) {
                // if hour = 23, then policy.start_time.hour = 24, we should start next backup,
                // because now - info.start_time_ms > policy.backup_interval
                ASSERT_TRUE(mp.should_start_backup_unlocked());
            } else {
                // should not start, even if now - info.start_time_ms > policy.backup_interval, but
                // not reach the time-point that policy.start_time limit
                ASSERT_FALSE(mp.should_start_backup_unlocked());
            }
        }

        {
            std::cout << "not first backup & no limit to start time & should start backup"
                      << std::endl;
            mp._policy.start_time.hour = 24;
            mp._backup_history.clear();
            info.start_time_ms = now - (oneday_sec * 1000) - 3 * 60 * 60;
            info.end_time_ms = info.start_time_ms + 10;
            mp.add_backup_history(info);
            ASSERT_TRUE(mp.should_start_backup_unlocked());
        }

        {
            std::cout << "not first backup & no limit to start time & should not start backup"
                      << std::endl;
            mp._backup_history.clear();
            info.start_time_ms = now - (oneday_sec * 1000) + 3 * 60 * 60;
            info.end_time_ms = info.start_time_ms + 10;
            mp.add_backup_history(info);
            ASSERT_FALSE(mp.should_start_backup_unlocked());
        }
    }
}

void meta_service_test_app::backup_service_test()
{
    std::shared_ptr<meta_service> meta_svc = std::make_shared<fake_receiver_meta_service>();
    meta_options &opt = meta_svc->_meta_opts;
    opt.cluster_root = "/meta_test";
    opt.meta_state_service_type = "meta_state_service_simple";
    meta_svc->remote_storage_initialize();
    std::string backup_root = "/backup_test";
    std::string policy_meta_root = opt.cluster_root + "/backup_policies";
    meta_svc->_backup_handler = std::make_shared<backup_service>(
        meta_svc.get(), policy_meta_root, backup_root, [](backup_service *bs) {
            return std::make_shared<mock_policy>(bs);
        });
    backup_service *backup_svc = meta_svc->_backup_handler.get();

    // first testing start_create_policy_meta_root()
    {
        std::cout << "testing start_create_policy_meta_root()..." << std::endl;
        bool flag = false;
        dsn::task_ptr task_test =
            tasking::create_task(LPC_DEFAULT_CALLBACK, nullptr, [&flag]() { flag = true; });
        backup_svc->start_create_policy_meta_root(task_test);
        while (!flag) {
            std::cout << "wait create policy_meta_root succeed" << std::endl;
            sleep(1);
        }
        ASSERT_TRUE(flag);
    }

    // testing add_new_policy()
    {
        std::cout << "add_new_policy()..." << std::endl;
        // create a fake add_backup_policy_request
        configuration_add_backup_policy_request req;
        req.backup_provider_type = std::string("local_service");
        req.policy_name = test_policy_name;
        req.app_ids = {1, 2, 3};
        req.backup_interval_seconds = 10;

        // case1: backup policy don't contain invalid app_id
        // result: backup policy will not be added, and return ERR_INVALID_PARAMETERS
        {
            configuration_add_backup_policy_response resp;
            auto r = fake_rpc_call(RPC_CM_ADD_BACKUP_POLICY,
                                   LPC_DEFAULT_CALLBACK,
                                   backup_svc,
                                   &backup_service::add_new_policy,
                                   req);
            fake_wait_rpc(r, resp);
            ASSERT_TRUE(resp.err == ERR_INVALID_PARAMETERS);
        }

        // case2: backup policy contains valid app_id
        // result: add backup policy succeed
        {
            configuration_add_backup_policy_response resp;
            server_state *state = meta_svc->get_server_state();
            state->_all_apps.insert(std::make_pair(1, std::make_shared<app_state>(app_info())));
            auto r = fake_rpc_call(RPC_CM_ADD_BACKUP_POLICY,
                                   LPC_DEFAULT_CALLBACK,
                                   backup_svc,
                                   &backup_service::add_new_policy,
                                   req);
            fake_wait_rpc(r, resp);
            ASSERT_TRUE(resp.err == ERR_OK);
            mock_policy *ptr =
                static_cast<mock_policy *>(backup_svc->_policy_states.at(test_policy_name).get());
            ASSERT_TRUE(ptr->counter_start() == 1);
        }
    }

    // testing sync_policies_from_remote_storage()
    // only have one backup policy on remote storage
    {
        std::cout << "tesing sync_policies_from_remote_storage()..." << std::endl;
        backup_svc->_policy_states.clear();
        ASSERT_TRUE(backup_svc->_policy_states.empty());
        error_code err = backup_svc->sync_policies_from_remote_storage();
        ASSERT_TRUE(err == ERR_OK);
        ASSERT_TRUE(backup_svc->_policy_states.size() == 1);
        ASSERT_TRUE(backup_svc->_policy_states.find(test_policy_name) !=
                    backup_svc->_policy_states.end());
        const policy &p = backup_svc->_policy_states.at(test_policy_name)->get_policy();
        ASSERT_TRUE(p.app_ids.size() == 1 && p.app_ids.count(1) == 1);
        ASSERT_TRUE(p.backup_provider_type == std::string("local_service"));
        ASSERT_TRUE(p.backup_interval_seconds == 10);
        ASSERT_TRUE(p.policy_name == test_policy_name);
    }
}
