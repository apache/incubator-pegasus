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

#include "replica/split/replica_split_manager.h"
#include "replica/test/replica_test_base.h"

#include <gtest/gtest.h>
#include <dsn/utility/fail_point.h>

namespace dsn {
namespace replication {

class replica_split_test : public testing::Test
{
public:
    void SetUp()
    {
        _stub = make_unique<mock_replica_stub>();
        _stub->set_state_connected();
        mock_app_info();
        _parent_replica = _stub->generate_replica(
            _app_info, _parent_pid, partition_status::PS_PRIMARY, _init_ballot);
        _parent_split_mgr = make_unique<replica_split_manager>(_parent_replica.get());
        mock_group_check_request();
    }

    void TearDown() {}

    void mock_app_info()
    {
        _app_info.app_id = 2;
        _app_info.app_name = "split_test";
        _app_info.app_type = "replica";
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }

    void mock_group_check_request()
    {
        _group_check_req.child_gpid = _child_pid;
        _group_check_req.config.ballot = _init_ballot;
        _group_check_req.config.status = partition_status::PS_PRIMARY;
    }

    void mock_notify_catch_up_request()
    {
        _parent_split_mgr->_child_gpid = _child_pid;
        _catch_up_req.child_gpid = _child_pid;
        _catch_up_req.parent_gpid = _parent_pid;
        _catch_up_req.child_ballot = _init_ballot;
        _catch_up_req.child_address = dsn::rpc_address("127.0.0.1", 1);
    }

    void mock_register_child_request()
    {
        partition_configuration &p_config = _register_req.parent_config;
        p_config.pid = _parent_pid;
        p_config.ballot = _init_ballot;
        p_config.last_committed_decree = _decree;

        partition_configuration &c_config = _register_req.child_config;
        c_config.pid = _child_pid;
        c_config.ballot = _init_ballot + 1;
        c_config.last_committed_decree = 0;

        _register_req.app = _app_info;
        _register_req.primary_address = dsn::rpc_address("127.0.0.1", 10086);
    }

    void generate_child(partition_status::type status)
    {
        _child_replica = _stub->generate_replica(_app_info, _child_pid, status, _init_ballot);
        _child_split_mgr = make_unique<replica_split_manager>(_child_replica.get());
        _parent_split_mgr->_child_gpid = _child_pid;
        _parent_split_mgr->_child_init_ballot = _init_ballot;
    }

    void mock_shared_log()
    {
        mock_mutation_log_shared_ptr shared_log_mock = new mock_mutation_log_shared("./");
        _stub->set_log(shared_log_mock);
    }

    void mock_private_log(gpid pid, mock_replica_ptr rep, bool mock_log_file_flag)
    {
        mock_mutation_log_private_ptr private_log_mock = new mock_mutation_log_private(pid, rep);
        if (mock_log_file_flag) {
            mock_log_file_ptr log_file_mock = new mock_log_file("log.1.0.txt", 0);
            log_file_mock->set_file_size(100);
            private_log_mock->add_log_file(log_file_mock);
        }
        rep->_private_log = private_log_mock;
    }

    void mock_prepare_list(mock_replica_ptr rep, bool add_to_plog)
    {
        _mock_plist = new prepare_list(rep, 1, _max_count, [](mutation_ptr mu) {});
        for (int i = 1; i < _max_count + 1; ++i) {
            mutation_ptr mu = new mutation();
            mu->data.header.decree = i;
            mu->data.header.ballot = _init_ballot;
            _mock_plist->put(mu);
            if (add_to_plog) {
                rep->_private_log->append(mu, LPC_WRITE_REPLICATION_LOG_PRIVATE, nullptr, nullptr);
                mu->set_logged();
            }
        }
        rep->_prepare_list = _mock_plist;
    }

    void mock_parent_states()
    {
        mock_shared_log();
        mock_private_log(_parent_pid, _parent_replica, true);
        mock_prepare_list(_parent_replica, true);
    }

    void mock_child_split_context(gpid parent_gpid, bool is_prepare_list_copied, bool is_caught_up)
    {
        _child_replica->_split_states.parent_gpid = parent_gpid;
        _child_replica->_split_states.is_prepare_list_copied = is_prepare_list_copied;
        _child_replica->_split_states.is_caught_up = is_caught_up;
    }

    void mock_mutation_list(decree min_decree)
    {
        // mock mutation list
        for (int d = 1; d < _max_count; ++d) {
            mutation_ptr mu = _mock_plist->get_mutation_by_decree(d);
            if (d > min_decree) {
                _mutation_list.push_back(mu);
            }
        }
    }

    void mock_parent_primary_context(bool will_all_caught_up)
    {
        _parent_replica->_primary_states.statuses[dsn::rpc_address("127.0.0.1", 1)] =
            partition_status::PS_PRIMARY;
        _parent_replica->_primary_states.statuses[dsn::rpc_address("127.0.0.1", 2)] =
            partition_status::PS_SECONDARY;
        _parent_replica->_primary_states.statuses[dsn::rpc_address("127.0.0.1", 3)] =
            partition_status::PS_SECONDARY;
        _parent_replica->_primary_states.caught_up_children.insert(
            dsn::rpc_address("127.0.0.1", 2));
        if (will_all_caught_up) {
            _parent_replica->_primary_states.caught_up_children.insert(
                dsn::rpc_address("127.0.0.1", 3));
        }
        _parent_replica->_primary_states.sync_send_write_request = false;
    }

    bool get_sync_send_write_request()
    {
        return _parent_replica->_primary_states.sync_send_write_request;
    }

    void
    mock_child_async_learn_states(mock_replica_ptr plist_rep, bool add_to_plog, decree min_decree)
    {
        mock_shared_log();
        mock_private_log(_child_pid, _child_replica, false);
        mock_prepare_list(plist_rep, add_to_plog);
        // mock_learn_state
        _mock_learn_state.to_decree_included = _decree;
        _mock_learn_state.files.push_back("fake_file_name");
        // mock parent private log files
        _private_log_files.push_back("log.1.0.txt");
        // mock mutation list
        mock_mutation_list(min_decree);
    }

    void cleanup_prepare_list(mock_replica_ptr rep) { rep->_prepare_list->reset(0); }

    void cleanup_child_split_context()
    {
        _child_replica->_split_states.cleanup(true);
        _child_replica->tracker()->wait_outstanding_tasks();
    }

    partition_split_context get_split_context() { return _child_replica->_split_states; }

    primary_context get_replica_primary_context(mock_replica_ptr rep)
    {
        return rep->_primary_states;
    }

    bool is_parent_not_in_split() { return (_parent_split_mgr->_child_gpid.get_app_id() == 0); }

    void test_on_add_child()
    {
        _parent_split_mgr->on_add_child(_group_check_req);
        _parent_replica->tracker()->wait_outstanding_tasks();
    }

    bool test_parent_check_states() { return _parent_split_mgr->parent_check_states(); }

    void test_child_copy_prepare_list()
    {
        mock_child_async_learn_states(_parent_replica, false, _decree);
        std::shared_ptr<prepare_list> plist =
            std::make_shared<prepare_list>(_parent_replica, *_mock_plist);
        _child_split_mgr->child_copy_prepare_list(_mock_learn_state,
                                                  _mutation_list,
                                                  _private_log_files,
                                                  _total_file_size,
                                                  std::move(plist));
        _child_replica->tracker()->wait_outstanding_tasks();
    }

    void test_child_learn_states()
    {
        mock_child_async_learn_states(_child_replica, true, _decree);
        _child_split_mgr->child_learn_states(
            _mock_learn_state, _mutation_list, _private_log_files, _total_file_size, _decree);
        _child_replica->tracker()->wait_outstanding_tasks();
    }

    void test_child_apply_private_logs()
    {
        mock_child_async_learn_states(_child_replica, true, 0);
        _child_split_mgr->child_apply_private_logs(
            _private_log_files, _mutation_list, _total_file_size, _decree);
        _child_replica->tracker()->wait_outstanding_tasks();
    }

    void test_child_catch_up_states(decree local_decree, decree goal_decree, decree min_decree)
    {
        mock_child_async_learn_states(_child_replica, true, 0);
        _child_replica->set_app_last_committed_decree(local_decree);
        if (local_decree < goal_decree) {
            // set prepare_list's start_decree = {min_decree}
            _child_replica->prepare_list_truncate(min_decree);
            // set prepare_list's last_committed_decree = {goal_decree}
            _child_replica->prepare_list_commit_hard(goal_decree);
        }
        _child_split_mgr->child_catch_up_states();
        _child_replica->tracker()->wait_outstanding_tasks();
    }

    dsn::error_code test_parent_handle_child_catch_up()
    {
        notify_cacth_up_response resp;
        _parent_split_mgr->parent_handle_child_catch_up(_catch_up_req, resp);
        _parent_replica->tracker()->wait_outstanding_tasks();
        return resp.err;
    }

    void test_register_child_on_meta()
    {
        _parent_split_mgr->register_child_on_meta(_init_ballot);
        _parent_replica->tracker()->wait_outstanding_tasks();
    }

    void test_on_register_child_rely(partition_status::type status, dsn::error_code resp_err)
    {
        mock_register_child_request();
        _parent_replica->_config.status = status;

        register_child_response resp;
        resp.err = resp_err;
        resp.app = _register_req.app;
        resp.app.partition_count *= 2;
        resp.parent_config = _register_req.parent_config;
        resp.child_config = _register_req.child_config;

        _parent_split_mgr->on_register_child_on_meta_reply(ERR_OK, _register_req, resp);
        _parent_replica->tracker()->wait_outstanding_tasks();
    }

public:
    std::unique_ptr<mock_replica_stub> _stub;

    mock_replica_ptr _parent_replica;
    mock_replica_ptr _child_replica;
    std::unique_ptr<replica_split_manager> _parent_split_mgr;
    std::unique_ptr<replica_split_manager> _child_split_mgr;

    dsn::app_info _app_info;
    dsn::gpid _parent_pid = gpid(2, 1);
    dsn::gpid _child_pid = gpid(2, 9);
    uint32_t _old_partition_count = 8;
    ballot _init_ballot = 3;
    decree _decree = 5;

    group_check_request _group_check_req;
    notify_catch_up_request _catch_up_req;
    register_child_request _register_req;
    std::vector<std::string> _private_log_files;
    std::vector<mutation_ptr> _mutation_list;
    const uint32_t _max_count = 10;
    prepare_list *_mock_plist;
    const uint64_t _total_file_size = 100;
    learn_state _mock_learn_state;
};

TEST_F(replica_split_test, add_child_wrong_ballot)
{
    ballot wrong_ballot = 5;
    _group_check_req.config.ballot = wrong_ballot;
    test_on_add_child();
    ASSERT_EQ(_stub->get_replica(_child_pid), nullptr);
}

TEST_F(replica_split_test, add_child_with_child_existed)
{
    _parent_split_mgr->set_child_gpid(_child_pid);
    test_on_add_child();
    ASSERT_EQ(_stub->get_replica(_child_pid), nullptr);
}

TEST_F(replica_split_test, add_child_succeed)
{
    fail::setup();
    fail::cfg("replica_stub_create_child_replica_if_not_found", "return()");
    fail::cfg("replica_child_init_replica", "return()");

    test_on_add_child();
    ASSERT_NE(_stub->get_replica(_child_pid), nullptr);
    _stub->get_replica(_child_pid)->tracker()->wait_outstanding_tasks();

    fail::teardown();
}

TEST_F(replica_split_test, parent_check_states_with_wrong_status)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    _parent_replica->set_partition_status(partition_status::PS_POTENTIAL_SECONDARY);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    bool flag = test_parent_check_states();
    ASSERT_FALSE(flag);
    fail::teardown();
}

TEST_F(replica_split_test, parent_check_states)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    bool flag = test_parent_check_states();
    ASSERT_TRUE(flag);
}

TEST_F(replica_split_test, copy_prepare_list_with_wrong_status)
{
    generate_child(partition_status::PS_INACTIVE);
    mock_child_split_context(_parent_pid, false, false);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    test_child_copy_prepare_list();
    fail::teardown();

    cleanup_prepare_list(_parent_replica);
    // TODO(heyuchen): child should be equal to error(after implement child_handle_split_error)
}

TEST_F(replica_split_test, copy_prepare_list_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, false, false);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    fail::cfg("replica_child_learn_states", "return()");
    test_child_copy_prepare_list();
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_prepare_list_copied, true);
    ASSERT_EQ(_child_replica->get_plist()->count(), _max_count);

    cleanup_prepare_list(_parent_replica);
    cleanup_prepare_list(_child_replica);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, false);

    fail::setup();
    fail::cfg("replica_child_apply_private_logs", "return()");
    fail::cfg("replica_child_catch_up_states", "return()");
    test_child_learn_states();
    fail::teardown();

    cleanup_prepare_list(_child_replica);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_with_replay_private_log_error)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, false);

    fail::setup();
    fail::cfg("replica_child_apply_private_logs", "return(error)");
    fail::cfg("replica_child_catch_up_states", "return()");
    test_child_learn_states();
    fail::teardown();

    cleanup_prepare_list(_child_replica);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, child_apply_private_logs_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, false);

    fail::setup();
    fail::cfg("mutation_log_replay_succeed", "return()");
    fail::cfg("replication_app_base_apply_mutation", "return()");
    test_child_apply_private_logs();
    fail::teardown();

    cleanup_prepare_list(_child_replica);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, catch_up_succeed_with_all_states_learned)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, false);

    fail::setup();
    fail::cfg("replica_child_notify_catch_up", "return()");
    test_child_catch_up_states(_decree, _decree, _decree);
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_caught_up, true);

    cleanup_prepare_list(_child_replica);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, catch_up_succeed_with_learn_in_memory_mutations)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, false);

    fail::setup();
    fail::cfg("replica_child_notify_catch_up", "return()");
    fail::cfg("replication_app_base_apply_mutation", "return()");
    test_child_catch_up_states(_decree, _max_count - 1, 1);
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_caught_up, true);

    cleanup_prepare_list(_child_replica);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, handle_catch_up_with_ballot_wrong)
{
    mock_notify_catch_up_request();
    _catch_up_req.child_ballot = 1;

    fail::setup();
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    dsn::error_code err = test_parent_handle_child_catch_up();
    fail::teardown();

    ASSERT_EQ(err, ERR_INVALID_STATE);
}

TEST_F(replica_split_test, handle_catch_up_with_not_all_caught_up)
{
    mock_parent_primary_context(false);
    mock_notify_catch_up_request();

    fail::setup();
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    dsn::error_code err = test_parent_handle_child_catch_up();
    fail::teardown();

    ASSERT_EQ(err, ERR_OK);
    ASSERT_FALSE(get_sync_send_write_request());
}

TEST_F(replica_split_test, handle_catch_up_with_all_caught_up)
{
    mock_parent_primary_context(true);
    mock_notify_catch_up_request();

    fail::setup();
    fail::cfg("replica_parent_check_sync_point_commit", "return()");
    dsn::error_code err = test_parent_handle_child_catch_up();
    fail::teardown();

    ASSERT_EQ(err, ERR_OK);
    ASSERT_TRUE(get_sync_send_write_request());
}

TEST_F(replica_split_test, register_child_test)
{
    fail::setup();
    fail::cfg("replica_parent_send_register_request", "return()");
    test_register_child_on_meta();
    fail::teardown();

    ASSERT_EQ(_parent_replica->status(), partition_status::PS_INACTIVE);
    ASSERT_EQ(_parent_split_mgr->get_partition_version(), -1);
}

TEST_F(replica_split_test, register_child_reply_with_wrong_status)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, true);

    test_on_register_child_rely(partition_status::PS_PRIMARY, ERR_OK);
    primary_context parent_primary_states = get_replica_primary_context(_parent_replica);
    ASSERT_EQ(parent_primary_states.register_child_task, nullptr);
}

TEST_F(replica_split_test, register_child_reply_with_child_registered)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, true);

    test_on_register_child_rely(partition_status::PS_INACTIVE, ERR_CHILD_REGISTERED);

    primary_context parent_primary_states = get_replica_primary_context(_parent_replica);
    ASSERT_EQ(parent_primary_states.register_child_task, nullptr);
    ASSERT_TRUE(is_parent_not_in_split());
}

TEST_F(replica_split_test, register_child_reply_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true, true);

    fail::setup();
    fail::cfg("replica_stub_split_replica_exec", "return()");
    test_on_register_child_rely(partition_status::PS_INACTIVE, ERR_OK);
    fail::teardown();

    ASSERT_TRUE(is_parent_not_in_split());
}

} // namespace replication
} // namespace dsn
