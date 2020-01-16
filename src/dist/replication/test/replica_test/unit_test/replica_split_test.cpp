// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/utility/fail_point.h>
#include "replica_test_base.h"

namespace dsn {
namespace replication {

class replica_split_test : public testing::Test
{
public:
    void SetUp()
    {
        _stub = make_unique<mock_replica_stub>();
        mock_app_info();
        _parent = _stub->generate_replica(
            _app_info, _parent_pid, partition_status::PS_PRIMARY, _init_ballot);
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
        _req.child_gpid = _child_pid;
        _req.config.ballot = _init_ballot;
        _req.config.status = partition_status::PS_PRIMARY;
    }

    void generate_child(partition_status::type status)
    {
        _child = _stub->generate_replica(_app_info, _child_pid, status, _init_ballot);
        _parent->set_child_gpid(_child_pid);
        _parent->set_init_child_ballot(_init_ballot);
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
        _mock_plist = new prepare_list(rep, 0, _max_count, nullptr);
        for (int i = 0; i < _max_count; ++i) {
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
        mock_private_log(_parent_pid, _parent, true);
        mock_prepare_list(_parent, true);
    }

    void mock_child_split_context(gpid parent_gpid, bool is_prepare_list_copied)
    {
        _child->_split_states.parent_gpid = parent_gpid;
        _child->_split_states.is_prepare_list_copied = is_prepare_list_copied;
    }

    void mock_child_async_learn_states(mock_replica_ptr plist_rep, bool add_to_plog)
    {
        mock_shared_log();
        mock_private_log(_child_pid, _child, false);
        mock_prepare_list(plist_rep, add_to_plog);
        // mock_learn_state
        _mock_learn_state.to_decree_included = _decree;
        _mock_learn_state.files.push_back("fake_file_name");
        // mock parent private log files
        _private_log_files.push_back("log.1.0.txt");
        // mock mutation list
        for (int d = 0; d < _max_count; ++d) {
            mutation_ptr mu = _mock_plist->get_mutation_by_decree(d);
            if (d > _decree) {
                _mutation_list.push_back(mu);
            }
        }
    }

    void cleanup_prepare_list(mock_replica_ptr rep) { rep->_prepare_list->reset(0); }

    void cleanup_child_split_context()
    {
        _child->_split_states.cleanup(true);
        _child->tracker()->wait_outstanding_tasks();
    }

    partition_split_context get_split_context() { return _child->_split_states; }

    void test_on_add_child()
    {
        _parent->on_add_child(_req);
        _parent->tracker()->wait_outstanding_tasks();
    }

    bool test_parent_check_states() { return _parent->parent_check_states(); }

    void test_parent_prepare_states()
    {
        _parent->parent_prepare_states(_parent->_app->learn_dir());
        _parent->tracker()->wait_outstanding_tasks();
    }

    void test_child_copy_prepare_list()
    {
        mock_child_async_learn_states(_parent, false);
        std::shared_ptr<prepare_list> plist = std::make_shared<prepare_list>(_parent, *_mock_plist);
        _child->child_copy_prepare_list(_mock_learn_state,
                                        _mutation_list,
                                        _private_log_files,
                                        _total_file_size,
                                        std::move(plist));
        _child->tracker()->wait_outstanding_tasks();
    }

    void test_child_learn_states()
    {
        mock_child_async_learn_states(_child, true);
        _child->child_learn_states(
            _mock_learn_state, _mutation_list, _private_log_files, _total_file_size, _decree);
        _child->tracker()->wait_outstanding_tasks();
    }

public:
    std::unique_ptr<mock_replica_stub> _stub;

    mock_replica_ptr _parent;
    mock_replica_ptr _child;

    dsn::app_info _app_info;
    dsn::gpid _parent_pid = gpid(2, 1);
    dsn::gpid _child_pid = gpid(2, 9);
    uint32_t _old_partition_count = 8;
    ballot _init_ballot = 3;
    decree _decree = 5;

    group_check_request _req;
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
    _req.config.ballot = wrong_ballot;
    test_on_add_child();
    ASSERT_EQ(_stub->get_replica(_child_pid), nullptr);
}

TEST_F(replica_split_test, add_child_with_child_existed)
{
    _parent->set_child_gpid(_child_pid);
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
    _parent->set_partition_status(partition_status::PS_POTENTIAL_SECONDARY);

    bool flag = test_parent_check_states();
    ASSERT_FALSE(flag);
}

TEST_F(replica_split_test, parent_check_states)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    bool flag = test_parent_check_states();
    ASSERT_TRUE(flag);
}

TEST_F(replica_split_test, parent_prepare_states_succeed)
{
    mock_parent_states();
    generate_child(partition_status::PS_PARTITION_SPLIT);

    fail::setup();
    fail::cfg("replica_parent_check_states", "return()");
    fail::cfg("replica_child_copy_prepare_list", "return()");
    test_parent_prepare_states();
    fail::teardown();

    cleanup_prepare_list(_parent);
}

TEST_F(replica_split_test, copy_prepare_list_with_wrong_status)
{
    generate_child(partition_status::PS_INACTIVE);
    mock_child_split_context(_parent_pid, false);

    fail::setup();
    fail::cfg("replica_child_learn_states", "return()");
    test_child_copy_prepare_list();
    fail::teardown();

    cleanup_prepare_list(_parent);
    // TODO(heyuchen): child should be equal to error(after implement child_handle_split_error)
}

TEST_F(replica_split_test, copy_prepare_list_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, false);

    fail::setup();
    fail::cfg("replica_child_learn_states", "return()");
    test_child_copy_prepare_list();
    fail::teardown();

    partition_split_context split_context = get_split_context();
    ASSERT_EQ(split_context.is_prepare_list_copied, true);
    ASSERT_EQ(_child->get_plist()->count(), _max_count);

    cleanup_prepare_list(_parent);
    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

TEST_F(replica_split_test, learn_states_succeed)
{
    generate_child(partition_status::PS_PARTITION_SPLIT);
    mock_child_split_context(_parent_pid, true);

    fail::setup();
    fail::cfg("replica_child_replay_private_log", "return()");
    fail::cfg("replica_child_learn_mutations", "return()");
    fail::cfg("replica_child_catch_up_states", "return()");
    test_child_learn_states();
    fail::teardown();

    cleanup_prepare_list(_child);
    cleanup_child_split_context();
}

// TODO(heyuchen): add learn_states failed case

} // namespace replication
} // namespace dsn
